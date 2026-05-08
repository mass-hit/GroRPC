package service

import (
	"GroRPC/codec"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

// MagicNumber is used to identify valid RPC requests
const MagicNumber = 0x01020304

type Option struct {
	MagicNumber    int
	ConnectTimeout time.Duration // connection timeout; 0 means no timeout
	HandelTimeout  time.Duration // request handling timeout; 0 means no timeout
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	ConnectTimeout: time.Second * 5,
}

// Server represents an RPC server
type Server struct {
	serviceMap sync.Map // map[string]*service
}

func NewServer() *Server {
	return &Server{}
}

// ServeConn handles a single connection
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer conn.Close()
	var option Option
	if err := json.NewDecoder(conn).Decode(&option); err != nil {
		log.Println("decode magic number:", err)
		return
	}
	number := option.MagicNumber
	if number != MagicNumber {
		log.Println("invalid magic number:", number)
		return
	}
	s.serveCodec(codec.NewGobCodec(conn), &option)
}

// serveCodec handles the request-response loop for a connection
func (s *Server) serveCodec(cc *codec.GobCodec, opt *Option) {
	sending := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for {
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			// Return error response to client
			req.header.Error = err.Error()
			s.sendResponse(cc, req.header, struct{}{}, sending)
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, opt.HandelTimeout)
	}
	wg.Wait()
	cc.Close()
}

// ServeHTTP enables RPC over HTTP
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only CONNECT method is supported
	if r.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Println(err)
		return
	}
	// Send successful CONNECT response
	_, _ = io.WriteString(conn, "HTTP/1.0 200 Connection established\r\n\r\n")
	s.ServeConn(conn)
}

const defaultHttpPath = "/grorpc"

func (s *Server) HandleHttp() {
	http.Handle(defaultHttpPath, s)
}

type request struct {
	header       *codec.Header
	argv, replyv reflect.Value
	mType        *methodType
	srv          *service
}

// readRequestHeader reads and decodes the request header
func (s *Server) readRequestHeader(cc *codec.GobCodec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// readRequest parses a RPC request
func (s *Server) readRequest(cc *codec.GobCodec) (*request, error) {
	header, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{header: header}
	req.srv, req.mType, err = s.findService(header.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mType.newArgv()
	req.replyv = req.mType.newReplyv()
	argi := req.argv.Interface()
	// Ensure pointer type for decoding
	if req.argv.Kind() != reflect.Ptr {
		argi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argi); err != nil {
		log.Println("read body error:", err)
	}
	return req, err
}

// sendResponse sends a response back to the client
func (s *Server) sendResponse(cc *codec.GobCodec, header *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(header, body); err != nil {
		log.Println("write error:", err)
	}
}

// handleRequest processes a RPC request
func (s *Server) handleRequest(cc *codec.GobCodec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	// Ensures only one response is sent
	var once sync.Once
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		if err := req.srv.call(req.mType, req.argv, req.replyv); err != nil {
			req.header.Error = err.Error()
			once.Do(func() { s.sendResponse(cc, req.header, struct{}{}, sending) })
			return
		}
		once.Do(func() { s.sendResponse(cc, req.header, req.replyv.Interface(), sending) })
		close(done)
	}()
	if timeout > 0 {
		select {
		case <-done:
		case <-ctx.Done():
			once.Do(func() { s.sendResponse(cc, req.header, struct{}{}, sending) })
		}
	} else {
		<-done
	}
}

// Accept listens for incoming connections
func (s *Server) Accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("accept error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

// Register registers a service to the server
func (s *Server) Register(rev interface{}) error {
	service := newService(rev)
	if _, dup := s.serviceMap.LoadOrStore(service.name, service); dup {
		return errors.New("service exists")
	}
	return nil
}

// findService resolves a service and method from "Service.Method"
func (s *Server) findService(serviceMethod string) (srv *service, mType *methodType, err error) {
	dot := strings.Index(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("invalid service method")
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	value, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("service not found")
		return
	}
	srv = value.(*service)
	mType = srv.methodMap[methodName]
	if mType == nil {
		err = errors.New("method not found")
	}
	return
}

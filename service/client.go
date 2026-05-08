package service

import (
	"GroRPC/codec"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
)

// Call represents an active RPC request
type Call struct {
	Seq           uint64
	ServiceMethod string // "User.Get"
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

type Client struct {
	cc       *codec.GobCodec
	option   *Option
	sending  sync.Mutex
	header   codec.Header
	mu       sync.Mutex
	seq      uint64
	pending  map[uint64]*Call
	closing  bool // user initiated close
	shutdown bool // server or internal shutdown
}

var ErrShutdown = errors.New("client shutdown")

// Close closes the client connection
func (cl *Client) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.closing {
		return ErrShutdown
	}
	cl.closing = true
	return cl.cc.Close()
}

// IsAvailable reports whether the client is still usable
func (cl *Client) IsAvailable() bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return !cl.closing && !cl.shutdown
}

// registerCall registers a new RPC call and assigns a sequence number
func (cl *Client) registerCall(call *Call) (uint64, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.closing || cl.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = cl.seq
	cl.pending[call.Seq] = call
	cl.seq++
	return call.Seq, nil
}

// removeCall removes a call from the pending map by sequence number
func (cl *Client) removeCall(seq uint64) *Call {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	call := cl.pending[seq]
	delete(cl.pending, seq)
	return call
}

// terminateCalls terminates all pending calls with the given error
func (cl *Client) terminateCalls(err error) {
	cl.sending.Lock()
	defer cl.sending.Unlock()
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.shutdown = true
	for _, call := range cl.pending {
		call.Error = err
		call.Done <- call
	}
}

// receive responses from the server
func (cl *Client) receive() {
	var err error
	for err == nil {
		var header codec.Header
		if err = cl.cc.ReadHeader(&header); err != nil {
			break
		}
		call := cl.removeCall(header.Seq)
		switch {
		case call == nil:
			err = cl.cc.ReadBody(nil)
		case header.Error != "":
			call.Error = errors.New(header.Error)
			err = cl.cc.ReadBody(nil)
			call.Done <- call
		default:
			if err = cl.cc.ReadBody(call.Reply); err != nil {
				call.Error = err
			}
			call.Done <- call
		}
	}
	// terminate all pending calls
	cl.terminateCalls(err)
}

// Map protocol name to client constructor
var clientFuncMap = make(map[string]newClientFunc)

func init() {
	clientFuncMap["http"] = NewHTTPClient
	clientFuncMap["tcp"] = NewClient
}

func NewClient(conn net.Conn, option *Option) (*Client, error) {
	if err := json.NewEncoder(conn).Encode(option); err != nil {
		// best-effort cleanup
		conn.Close()
		return nil, err
	}
	client := &Client{
		seq:     1,
		cc:      codec.NewGobCodec(conn),
		option:  DefaultOption,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client, nil
}

// NewHTTPClient establishes an RPC client over HTTP
func NewHTTPClient(conn net.Conn, option *Option) (*Client, error) {
	// Send HTTP CONNECT request
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultHttpPath))
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && response.StatusCode == 200 {
		return NewClient(conn, option)
	}
	if err == nil {
		err = errors.New(response.Status)
	}
	return nil, err
}

// parseOption normalizes user-provided Option
func parseOption(option *Option) (*Option, error) {
	if option == nil {
		return DefaultOption, nil
	}
	option.MagicNumber = DefaultOption.MagicNumber
	return option, nil
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(net.Conn, *Option) (*Client, error)

// dialTimeout establishes a connection with timeout
func dialTimeout(clientFunc newClientFunc, addr string, option *Option) (client *Client, err error) {
	opt, err := parseOption(option)
	if err != nil {
		return nil, err
	}
	connectTimeout := opt.ConnectTimeout
	var ctx context.Context
	var cancel context.CancelFunc
	if connectTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), connectTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	// ensure the connection is closed if client initialization fails
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()
	// buffered channel prevents goroutine leak if timeout occurs
	ch := make(chan *clientResult, 1)
	go func() {
		cl, clientErr := clientFunc(conn, opt)
		ch <- &clientResult{cl, clientErr}
	}()
	if connectTimeout > 0 {
		select {
		case result := <-ch:
			return result.client, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		result := <-ch
		return result.client, result.err
	}
}

// Dial creates a client using the protocol
func Dial(protocol, addr string, option *Option) (cl *Client, err error) {
	clientFunc, ok := clientFuncMap[protocol]
	if !ok {
		return nil, fmt.Errorf("protocol %s not supported", protocol)
	}
	return dialTimeout(clientFunc, addr, option)
}

// send an RPC request
func (cl *Client) send(call *Call) {
	cl.sending.Lock()
	defer cl.sending.Unlock()
	seq, err := cl.registerCall(call)
	if err != nil {
		call.Error = err
		call.Done <- call
		return
	}
	cl.header.Seq = seq
	cl.header.ServiceMethod = call.ServiceMethod
	cl.header.Error = ""
	if err = cl.cc.Write(&cl.header, call.Args); err != nil {
		call = cl.removeCall(call.Seq)
		if call != nil {
			call.Error = err
			call.Done <- call
		}
	}
}

// Go invokes an RPC asynchronously
func (cl *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 1)
	} else if cap(done) == 0 {
		log.Panic("done channel is unbuffered")
	}
	call := &Call{
		Seq:           cl.seq,
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	cl.send(call)
	return call
}

// Call invokes an RPC synchronously with context
func (cl *Client) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	call := cl.Go(serviceMethod, args, reply, nil)
	select {
	case call = <-call.Done:
		return call.Error
	case <-ctx.Done():
		cl.removeCall(call.Seq)
		return ctx.Err()
	}
}

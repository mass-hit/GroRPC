package registry

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Registry is a registry center
type Registry struct {
	timeout time.Duration // server expiration timeout
	mu      sync.Mutex
	servers map[string]*ServerItem
}

// ServerItem stores metadata for a server
type ServerItem struct {
	Addr  string
	start time.Time
}

const DefaultTimeout = 10 * time.Second

func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

// putServer registers a server
func (r *Registry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{addr, time.Now()}
	} else {
		s.start = time.Now()
	}
}

// aliveServers returns all active servers
func (r *Registry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || time.Since(s.start) <= r.timeout {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	return alive
}

// ServeHTTP implements the registry HTTP API
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("GroRPC-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("GroRPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Registry) HandleHttp(path string) {
	http.Handle(path, r)
}

// HeartBeat periodically sends heartbeat
func HeartBeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = DefaultTimeout
	}
	err := sendHeartBeat(registry, addr)
	go func() {
		ticker := time.NewTicker(duration)
		for err == nil {
			<-ticker.C
			err = sendHeartBeat(registry, addr)
		}
	}()
}

func sendHeartBeat(registry, addr string) error {
	log.Printf("addr %s Sending heartbeat to %s", addr, registry)
	httpClient := &http.Client{}
	req, err := http.NewRequest("POST", registry, nil)
	if err != nil {
		log.Println("sendHeartBeat error:", err)
		return err
	}
	req.Header.Set("GroRPC-Server", addr)
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("sendHeartBeat error:", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

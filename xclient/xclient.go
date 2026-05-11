package xclient

import (
	"GroRPC/service"
	"context"
	"log"
	"reflect"
	"sync"
)

// XClient is an extended RPC client
type XClient struct {
	d       Discovery
	opt     *service.Option
	mu      sync.Mutex
	clients map[string]*service.Client // Cached RPC clients indexed by server address
}

func NewXClient(d Discovery, opt *service.Option) *XClient {
	return &XClient{d: d, opt: opt, clients: make(map[string]*service.Client)}
}

// Close closes all cached RPC client connections
func (c *XClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, client := range c.clients {
		client.Close()
		delete(c.clients, key)
	}
	return nil
}

// dial returns a client for the target server
func (c *XClient) dial(addr string) (client *service.Client, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, ok := c.clients[addr]
	if ok && !client.IsAvailable() {
		client.Close()
		delete(c.clients, addr)
		client = nil
	}
	// Lazily establish new connection
	if client == nil {
		client, err = service.Dial("tcp", addr, c.opt)
		if err != nil {
			return
		}
		c.clients[addr] = client
	}
	return
}

// Call performs RPC call using load balancing
func (c *XClient) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) (err error) {
	addr, err := c.d.Get()
	log.Printf("MultiServerDiscovery get addr: %s", addr)
	if err != nil {
		return
	}
	return c.call(ctx, addr, serviceMethod, args, reply)
}

// call sends RPC request to the server
func (c *XClient) call(ctx context.Context, addr, serviceMethod string, args interface{}, reply interface{}) (err error) {
	client, err := c.dial(addr)
	if err != nil {
		return
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// Broadcast sends RPC request to ALL servers concurrently.
func (c *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) (err error) {
	all, err := c.d.GetAll()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	replyDone := reply == nil
	// If any RPC call fails, remaining calls are canceled early
	ctx, cancel := context.WithCancel(ctx)
	for _, addr := range all {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var cloneReply interface{}
			// Each goroutine must use its own reply instance
			if reply != nil {
				cloneReply = reflect.New(reflect.TypeOf(reply).Elem()).Interface()
			}
			e := c.call(ctx, addr, serviceMethod, args, cloneReply)
			mu.Lock()
			defer mu.Unlock()
			// First error triggers cancellation
			if err == nil && e != nil {
				err = e
				cancel()
			}
			// First successful reply
			if e == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(cloneReply).Elem())
				replyDone = true
			}
		}(addr)
	}
	wg.Wait()
	// Ensure context resources are released
	cancel()
	return
}

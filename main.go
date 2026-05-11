package main

import (
	"GroRPC/registry"
	"GroRPC/service"
	"GroRPC/xclient"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

var serverNum int

type Server struct {
	num int
}

func (g *Server) Sum(argv int, reply *int) error {
	*reply = argv + g.num
	return nil
}

func (g *Server) Sleep(argv int, reply *int) error {
	time.Sleep(time.Second * 5)
	*reply = argv + g.num
	return nil
}

func startServer(addr, registryAddr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	srv := service.NewServer()
	serverNum++
	srv.Register(&Server{num: serverNum})
	registry.HeartBeat(registryAddr, addr, 0)
	go srv.Accept(l)
}

func main() {
	addr1, addr2, addr3, registryPort := ":8080", ":8081", ":8082", ":8003"
	r := registry.NewRegistry(0)
	l, err := net.Listen("tcp", registryPort)
	if err != nil {
		log.Fatal(err)
	}
	registryPath := "/grorpc/registry"
	r.HandleHttp(registryPath)
	go func() {
		err = http.Serve(l, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
	time.Sleep(time.Second)
	registryAddr := "http://localhost" + registryPort + registryPath
	discovery := xclient.NewRegistryDiscovery(registryAddr, 0)
	startServer(addr1, registryAddr)
	startServer(addr2, registryAddr)
	startServer(addr3, registryAddr)
	client := xclient.NewXClient(discovery, service.DefaultOption)
	defer client.Close()
	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var reply int
			err := client.Call(context.Background(), "Server.Sum", i, &reply)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(reply)
		}(i)
	}
	wg.Wait()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var reply int
			err := client.Broadcast(context.Background(), "Server.Sum", i, &reply)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(reply)
		}(i)
	}
	wg.Wait()
	time.Sleep(30 * time.Second)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var reply int
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := client.Broadcast(ctx, "Server.Sleep", i, &reply)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(reply)
		}(i)
	}
	wg.Wait()
}

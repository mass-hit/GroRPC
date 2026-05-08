package main

import (
	service "GroRPC/service"
	"GroRPC/xclient"
	"context"
	"fmt"
	"log"
	"net"
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

func startServer(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	srv := service.NewServer()
	serverNum++
	srv.Register(&Server{num: serverNum})
	go srv.Accept(l)
}

func main() {
	addr1, addr2, addr3 := ":8080", ":8081", ":8082"
	startServer(addr1)
	startServer(addr2)
	startServer(addr3)
	d := xclient.NewMultiServerDiscovery([]string{addr1, addr2, addr3})
	_ = d.SetWeight(addr1, 3)
	_ = d.SetWeight(addr2, 2)
	client := xclient.NewXClient(d, service.DefaultOption)
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

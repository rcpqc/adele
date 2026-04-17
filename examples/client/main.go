package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rcpqc/adele/client"
)

func main() {
	clientID := flag.String("id", "client-001", "client ID")
	localAddr := flag.String("local", "localhost:80", "local HTTP service address")
	serverAddr := flag.String("server", "localhost:50051", "adele server address (host:port)")
	flag.Parse()

	// 创建客户端
	c := client.New(*clientID, *localAddr,
		client.WithServerAddress(*serverAddr),
		client.WithHeartbeatInterval(30*time.Second),
	)

	// 连接服务器
	if err := c.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Disconnect()

	log.Printf("[Client] Connected to server %s", *serverAddr)
	log.Printf("[Client] Local service: %s", *localAddr)
	log.Printf("[Client] Proxy address: http://<server-ip>%s", c.ProxyAddr())

	// 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[Client] Shutting down...")
}

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rcpqc/adele/server"
)

func main() {
	grpcAddr := flag.String("grpc", ":50051", "gRPC listen address")
	httpStart := flag.Int("http-start", 8081, "HTTP proxy port start")
	httpEnd := flag.Int("http-end", 8090, "HTTP proxy port end")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file (empty = plain HTTP)")
	tlsKey := flag.String("tls-key", "", "TLS private key file")
	flag.Parse()

	// 创建服务端
	opts := []server.Option{
		server.WithGRPCAddress(*grpcAddr),
		server.WithHTTPPortRange(*httpStart, *httpEnd),
		server.WithHeartbeatTimeout(60000),
	}
	if *tlsCert != "" {
		opts = append(opts, server.WithTLSCert(*tlsCert, *tlsKey))
	}
	s := server.New(opts...)

	// 启动服务端
	go func() {
		if err := s.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	log.Printf("[Server] gRPC listening on %s", *grpcAddr)
	log.Printf("[Server] HTTP proxy ports: %d-%d", *httpStart, *httpEnd)
	log.Println("[Server] Waiting for clients...")

	// 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[Server] Shutting down...")
	s.Stop()
}

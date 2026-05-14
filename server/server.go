package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rcpqc/adele/internal/proto"
	"google.golang.org/grpc"
)

// Server Adele服务端
type Server struct {
	proto.UnimplementedAdeleServiceServer

	grpcServer *grpc.Server
	listener   net.Listener

	mu      sync.RWMutex
	clients map[string]*clientConn // sessionID -> clientConn

	portPool *PortPool
	opts     *Options
}

// clientConn 客户端连接
type clientConn struct {
	info       *ClientInfo
	stream     proto.AdeleService_ConnectServer
	pending    map[string]chan *proto.HTTPResponse // requestID -> response channel
	pendingMu  sync.RWMutex
	httpServer *http.Server // 该客户端的HTTP代理服务
	cancel     context.CancelFunc
}

// ClientInfo 客户端信息
type ClientInfo struct {
	ClientID    string
	SessionID   string
	LocalAddr   string // 客户端本地HTTP服务地址
	ProxyAddr   string // 服务端分配的代理地址
	ConnectedAt time.Time
	LastSeen    time.Time
}

// Options 服务端配置选项
type Options struct {
	GRPCAddress      string // gRPC监听地址
	HTTPPortStart    int    // HTTP代理端口起始
	HTTPPortEnd      int    // HTTP代理端口结束
	HeartbeatTimeout int64  // 心跳超时(毫秒)
}

// Option 配置函数
type Option func(*Options)

// DefaultOptions 默认配置
func DefaultOptions() *Options {
	return &Options{
		GRPCAddress:      ":50051",
		HTTPPortStart:    8081,
		HTTPPortEnd:      8090,
		HeartbeatTimeout: 60000,
	}
}

// WithGRPCAddress 设置gRPC监听地址
func WithGRPCAddress(addr string) Option {
	return func(o *Options) { o.GRPCAddress = addr }
}

// WithHTTPPortRange 设置HTTP代理端口范围
func WithHTTPPortRange(start, end int) Option {
	return func(o *Options) {
		o.HTTPPortStart = start
		o.HTTPPortEnd = end
	}
}

// WithHeartbeatTimeout 设置心跳超时
func WithHeartbeatTimeout(timeout int64) Option {
	return func(o *Options) { o.HeartbeatTimeout = timeout }
}

// New 创建服务端实例
func New(opts ...Option) *Server {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	s := &Server{
		opts:     options,
		clients:  make(map[string]*clientConn),
		portPool: NewPortPool(options.HTTPPortStart, options.HTTPPortEnd),
	}

	s.grpcServer = grpc.NewServer()
	proto.RegisterAdeleServiceServer(s.grpcServer, s)
	return s
}

// Start 启动服务
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.opts.GRPCAddress)
	if err != nil {
		return err
	}
	s.listener = lis

	log.Printf("[Adele] gRPC server starting on %s", s.opts.GRPCAddress)

	go s.heartbeatChecker()

	return s.grpcServer.Serve(lis)
}

// Stop 停止服务
func (s *Server) Stop() {
	log.Println("[Adele] Server stopping...")

	s.mu.Lock()
	for _, cc := range s.clients {
		cc.cancel()
	}
	s.mu.Unlock()

	s.grpcServer.GracefulStop()
}

// Connect 双向流连接
func (s *Server) Connect(stream proto.AdeleService_ConnectServer) error {
	var sessionID string
	var cc *clientConn

	ctx, cancel := context.WithCancel(stream.Context())

	for {
		select {
		case <-ctx.Done():
			if sessionID != "" {
				s.unregister(sessionID)
			}
			return ctx.Err()
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			if sessionID != "" {
				s.unregister(sessionID)
			}
			cancel()
			return err
		}

		switch payload := msg.Payload.(type) {
		case *proto.ClientMessage_Register:
			sessionID, cc = s.handleRegister(stream, payload.Register, cancel)
		case *proto.ClientMessage_Heartbeat:
			s.handleHeartbeat(payload.Heartbeat, sessionID)
		case *proto.ClientMessage_HttpResponse:
			s.handleHTTPResponse(payload.HttpResponse, cc)
		}
	}
}

// handleRegister 处理客户端注册
func (s *Server) handleRegister(stream proto.AdeleService_ConnectServer, req *proto.RegisterRequest, cancel context.CancelFunc) (string, *clientConn) {
	// 检查是否已存在
	s.mu.RLock()
	for _, cc := range s.clients {
		if cc.info.ClientID == req.ClientId {
			s.mu.RUnlock()
			stream.Send(&proto.ServerMessage{
				Payload: &proto.ServerMessage_Register{
					Register: &proto.RegisterResponse{},
				},
			})
			return "", nil
		}
	}
	s.mu.RUnlock()

	// 分配端口
	port, err := s.portPool.Allocate()
	if err != nil {
		log.Printf("[Adele] Failed to allocate port for %s: %v", req.ClientId, err)
		stream.Send(&proto.ServerMessage{
			Payload: &proto.ServerMessage_Register{
				Register: &proto.RegisterResponse{},
			},
		})
		return "", nil
	}

	sessionID := generateSessionID()
	proxyAddr := fmt.Sprintf(":%d", port)

	info := &ClientInfo{
		ClientID:    req.ClientId,
		SessionID:   sessionID,
		LocalAddr:   req.LocalAddr,
		ProxyAddr:   proxyAddr,
		ConnectedAt: time.Now(),
		LastSeen:    time.Now(),
	}

	cc := &clientConn{
		info:    info,
		stream:  stream,
		pending: make(map[string]chan *proto.HTTPResponse),
		cancel:  cancel,
	}

	s.mu.Lock()
	s.clients[sessionID] = cc
	s.mu.Unlock()

	// 启动该客户端的HTTP代理服务
	go s.startHTTPProxy(port, cc)

	stream.Send(&proto.ServerMessage{
		Payload: &proto.ServerMessage_Register{
			Register: &proto.RegisterResponse{
				SessionId: sessionID,
				ProxyAddr: proxyAddr,
			},
		},
	})

	log.Printf("[Adele] Client registered: %s, local: %s, proxy: %s", req.ClientId, req.LocalAddr, proxyAddr)
	return sessionID, cc
}

// handleHeartbeat 处理心跳
func (s *Server) handleHeartbeat(req *proto.HeartbeatRequest, sessionID string) {
	s.mu.RLock()
	cc, ok := s.clients[sessionID]
	s.mu.RUnlock()

	if !ok {
		return
	}

	cc.info.LastSeen = time.Now()

	cc.stream.Send(&proto.ServerMessage{
		Payload: &proto.ServerMessage_Heartbeat{
			Heartbeat: &proto.HeartbeatResponse{
				ServerTime: time.Now().UnixMilli(),
			},
		},
	})
}

// handleHTTPResponse 处理HTTP响应
func (s *Server) handleHTTPResponse(resp *proto.HTTPResponse, cc *clientConn) {
	if cc == nil {
		return
	}

	cc.pendingMu.RLock()
	ch, ok := cc.pending[resp.RequestId]
	cc.pendingMu.RUnlock()

	if ok {
		ch <- resp
	}
}

// startHTTPProxy 启动HTTP代理服务
func (s *Server) startHTTPProxy(port int, cc *clientConn) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.proxyHTTP(w, r, cc)
	})

	addr := fmt.Sprintf(":%d", port)
	cc.httpServer = &http.Server{Addr: addr, Handler: handler}
	cc.info.ProxyAddr = addr

	log.Printf("[Adele] HTTP proxy starting on %s for client %s", addr, cc.info.ClientID)

	if err := cc.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[Adele] HTTP proxy error on %s: %v", addr, err)
	}
}

// proxyHTTP 代理HTTP请求到客户端
func (s *Server) proxyHTTP(w http.ResponseWriter, r *http.Request, cc *clientConn) {
	// 读取请求体
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	// 构造请求头
	headers := make(map[string]string)
	for k, vs := range r.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	requestID := generateRequestID()

	// 创建响应等待channel
	respCh := make(chan *proto.HTTPResponse, 1)
	cc.pendingMu.Lock()
	cc.pending[requestID] = respCh
	cc.pendingMu.Unlock()

	defer func() {
		cc.pendingMu.Lock()
		delete(cc.pending, requestID)
		cc.pendingMu.Unlock()
	}()

	// 通过gRPC流发送HTTP请求到客户端
	err := cc.stream.Send(&proto.ServerMessage{
		Payload: &proto.ServerMessage_HttpRequest{
			HttpRequest: &proto.HTTPRequest{
				RequestId: requestID,
				Method:    r.Method,
				Path:      r.URL.Path,
				Headers:   headers,
				Body:      body,
				Query:     r.URL.RawQuery,
			},
		},
	})
	if err != nil {
		log.Printf("[Adele] Failed to send request to client %s: %v", cc.info.ClientID, err)
		http.Error(w, "tunnel error", http.StatusBadGateway)
		return
	}

	log.Printf("[Adele] Proxy %s %s -> client %s", r.Method, r.URL.Path, cc.info.ClientID)

	// 等待响应，带超时
	select {
	case resp := <-respCh:
		for k, v := range resp.Headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(int(resp.StatusCode))
		w.Write(resp.Body)
		log.Printf("[Adele] Response %d for %s %s from client %s (%d bytes)",
			resp.StatusCode, r.Method, r.URL.Path, cc.info.ClientID, len(resp.Body))

	case <-time.After(30 * time.Second):
		log.Printf("[Adele] Request timeout for %s %s to client %s", r.Method, r.URL.Path, cc.info.ClientID)
		http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
	}
}

// unregister 注销客户端
func (s *Server) unregister(sessionID string) {
	s.mu.Lock()
	cc, ok := s.clients[sessionID]
	if !ok {
		s.mu.Unlock()
		return
	}
	delete(s.clients, sessionID)
	s.mu.Unlock()

	// 关闭HTTP代理服务，等待正在处理的请求完成
	if cc.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cc.httpServer.Shutdown(ctx)
		cancel()
	}

	// 解析端口
	_, portStr, _ := net.SplitHostPort(cc.info.ProxyAddr)
	port, _ := strconv.Atoi(portStr)

	// 等待端口真正释放（最多3秒），避免 TIME_WAIT 导致复用失败
	for i := 0; i < 30; i++ {
		ln, err := net.Listen("tcp", cc.info.ProxyAddr)
		if err == nil {
			ln.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 放回端口池
	s.portPool.Release(port)

	log.Printf("[Adele] Client unregistered: %s (proxy: %s)", cc.info.ClientID, cc.info.ProxyAddr)
}

// heartbeatChecker 心跳检测
func (s *Server) heartbeatChecker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		for _, cc := range s.clients {
			if time.Since(cc.info.LastSeen) > time.Duration(s.opts.HeartbeatTimeout)*time.Millisecond {
				log.Printf("[Adele] Client %s heartbeat timeout", cc.info.ClientID)
				cc.cancel()
			}
		}
		s.mu.RUnlock()
	}
}

// ListClients 列出所有客户端
func (s *Server) ListClients() []*ClientInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*ClientInfo, 0, len(s.clients))
	for _, cc := range s.clients {
		result = append(result, cc.info)
	}
	return result
}

func generateSessionID() string {
	return "sess_" + time.Now().Format("20060102150405") + "_" + randomString(8)
}

func generateRequestID() string {
	return "req_" + time.Now().Format("20060102150405.000") + "_" + randomString(4)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().Nanosecond()%len(letters)]
	}
	return string(b)
}

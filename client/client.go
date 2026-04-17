package client

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/rcpqc/adele/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client Adele客户端
type Client struct {
	conn   *grpc.ClientConn
	client proto.AdeleServiceClient
	stream proto.AdeleService_ConnectClient

	clientID  string
	sessionID string
	localAddr string
	proxyAddr string

	sendMu sync.Mutex

	opts *Options

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Options 客户端配置
type Options struct {
	ServerAddress     string
	HeartbeatInterval time.Duration
	ReconnectDelay    time.Duration
	MaxReconnectRetry int
}

// Option 配置函数
type Option func(*Options)

// DefaultOptions 默认配置
func DefaultOptions() *Options {
	return &Options{
		ServerAddress:     "localhost:50051",
		HeartbeatInterval: 30 * time.Second,
		ReconnectDelay:    5 * time.Second,
		MaxReconnectRetry: 10,
	}
}

// WithServerAddress 设置服务器地址
func WithServerAddress(addr string) Option {
	return func(o *Options) { o.ServerAddress = addr }
}

// WithHeartbeatInterval 设置心跳间隔
func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *Options) { o.HeartbeatInterval = d }
}

// New 创建客户端实例
func New(clientID string, localAddr string, opts ...Option) *Client {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Client{
		clientID:  clientID,
		localAddr: localAddr,
		opts:      options,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Connect 连接服务器
func (c *Client) Connect() error {
	conn, err := grpc.Dial(c.opts.ServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn
	c.client = proto.NewAdeleServiceClient(conn)

	stream, err := c.client.Connect(c.ctx)
	if err != nil {
		return err
	}
	c.stream = stream

	// 发送注册请求
	if err := c.register(); err != nil {
		return err
	}

	// 启动接收协程
	c.wg.Add(1)
	go c.recvLoop()

	// 启动心跳协程
	c.wg.Add(1)
	go c.heartbeatLoop()

	log.Printf("[Adele] Client %s connected, session: %s, proxy: %s", c.clientID, c.sessionID, c.proxyAddr)
	return nil
}

// Disconnect 断开连接
func (c *Client) Disconnect() {
	c.cancel()
	c.wg.Wait()

	if c.stream != nil {
		c.stream.CloseSend()
	}
	if c.conn != nil {
		c.conn.Close()
	}

	log.Printf("[Adele] Client %s disconnected", c.clientID)
}

// ProxyAddr 获取代理地址
func (c *Client) ProxyAddr() string {
	return c.proxyAddr
}

// register 注册客户端
func (c *Client) register() error {
	msg := &proto.ClientMessage{
		Payload: &proto.ClientMessage_Register{
			Register: &proto.RegisterRequest{
				ClientId:  c.clientID,
				LocalAddr: c.localAddr,
			},
		},
	}

	if err := c.stream.Send(msg); err != nil {
		return err
	}

	// 等待注册响应
	resp, err := c.stream.Recv()
	if err != nil {
		return err
	}

	regResp := resp.GetRegister()
	if regResp == nil {
		return io.ErrUnexpectedEOF
	}

	c.sessionID = regResp.SessionId
	c.proxyAddr = regResp.ProxyAddr
	return nil
}

// recvLoop 接收消息循环
func (c *Client) recvLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.stream.Recv()
			if err != nil {
				if err != io.EOF {
					log.Printf("[Adele] Receive error: %v", err)
				}
				return
			}

			c.handleMessage(msg)
		}
	}
}

// handleMessage 处理服务端消息
func (c *Client) handleMessage(msg *proto.ServerMessage) {
	switch payload := msg.Payload.(type) {
	case *proto.ServerMessage_HttpRequest:
		c.handleHTTPRequest(payload.HttpRequest)
	case *proto.ServerMessage_Heartbeat:
		// 心跳响应
	}
}

// handleHTTPRequest 处理HTTP请求转发
func (c *Client) handleHTTPRequest(req *proto.HTTPRequest) {
	// 构造本地HTTP请求
	url := "http://" + c.localAddr + req.Path
	if req.Query != "" {
		url += "?" + req.Query
	}

	bodyReader := bytes.NewReader(req.Body)
	httpReq, err := http.NewRequest(req.Method, url, bodyReader)
	if err != nil {
		c.sendHTTPResponse(req.RequestId, 500, nil, []byte(err.Error()))
		return
	}

	// 设置请求头
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	log.Printf("[Adele] Proxy %s %s -> %s", req.Method, req.Path, url)

	// 发送请求到本地HTTP服务
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		log.Printf("[Adele] Local request error: %v", err)
		c.sendHTTPResponse(req.RequestId, 502, nil, []byte(err.Error()))
		return
	}
	defer resp.Body.Close()

	// 读取响应体
	respBody, _ := io.ReadAll(resp.Body)

	// 收集响应头
	respHeaders := make(map[string]string)
	for k, vs := range resp.Header {
		if len(vs) > 0 {
			respHeaders[k] = vs[0]
		}
	}

	c.sendHTTPResponse(req.RequestId, int32(resp.StatusCode), respHeaders, respBody)
	log.Printf("[Adele] Response %d for %s %s (%d bytes)", resp.StatusCode, req.Method, req.Path, len(respBody))
}

// sendHTTPResponse 发送HTTP响应到服务端
func (c *Client) sendHTTPResponse(requestID string, statusCode int32, headers map[string]string, body []byte) {
	msg := &proto.ClientMessage{
		Payload: &proto.ClientMessage_HttpResponse{
			HttpResponse: &proto.HTTPResponse{
				RequestId:  requestID,
				StatusCode: statusCode,
				Headers:    headers,
				Body:       body,
			},
		},
	}
	c.stream.Send(msg)
}

// heartbeatLoop 心跳循环
func (c *Client) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.opts.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.sendHeartbeat()
		}
	}
}

// sendHeartbeat 发送心跳
func (c *Client) sendHeartbeat() {
	msg := &proto.ClientMessage{
		Payload: &proto.ClientMessage_Heartbeat{
			Heartbeat: &proto.HeartbeatRequest{
				SessionId: c.sessionID,
				Timestamp: time.Now().UnixMilli(),
			},
		},
	}
	c.stream.Send(msg)
}

# Adele

HTTP 反向代理隧道 - 通过 gRPC 将本地 HTTP 服务暴露到外网

## 简介

Adele 是一个轻量级的反向代理隧道工具，让你可以通过外网服务器访问本地运行的 HTTP 服务，无需公网 IP、无需域名、无需备案。

```
用户浏览器 → CVM:8081 → gRPC隧道 → 本地客户端 → 127.0.0.1:80
```

## 快速开始

### 1. 编译

```bash
# 服务端 (Linux AMD64，用于 CVM)
GOOS=linux GOARCH=amd64 go build -o adele-server examples/server/main.go

# 客户端 (macOS，用于本地开发)
go build -o adele-client-darwin examples/client/main.go

# 客户端 (Linux，用于本地)
GOOS=linux GOARCH=amd64 go build -o adele-client examples/client/main.go
```

### 2. 部署服务端到 CVM

```bash
# 上传二进制到 CVM
scp adele-server root@YOUR_CVM_IP:/usr/local/bin/

# SSH 登录后启动
ssh root@YOUR_CVM_IP
adele-server -grpc ":50051" -http-start 8081 -http-end 8090
```

**安全组配置：**
- 端口 `50051` → 允许你的本地 IP (gRPC 通信)
- 端口 `8081-8090` → 允许所有人 (HTTP 代理)

### 3. 本地启动客户端

```bash
# Mac
./adele-client-darwin -id myapp -local "127.0.0.1:80" -server "YOUR_CVM_IP:50051"

# Linux
./adele-client -id myapp -local "127.0.0.1:80" -server "YOUR_CVM_IP:50051"
```

### 4. 访问

客户端会显示代理地址，例如：
```
[Client] Proxy address: http://<server-ip>:8081
```

任何人都可以通过 `http://YOUR_CVM_IP:8081` 访问你本地的 `127.0.0.1:80` 服务。

---

## 命令行参数

### 服务端

```
-grpc string      gRPC listen address (default ":50051")
-http-start int   HTTP proxy port start (default 8081)
-http-end int     HTTP proxy port end (default 8090)
```

### 客户端

```
-id string        client ID (default "client-001")
-local string     local HTTP service address (default "localhost:80")
-server string    adele server address (default "localhost:50051")
```

---

## Systemd 服务配置

创建 `/etc/systemd/system/adele-server.service`：

```ini
[Unit]
Description=Adele Tunnel Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/adele-server -grpc ":50051" -http-start 8081 -http-end 8090
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

启动：
```bash
systemctl daemon-reload
systemctl enable adele-server
systemctl start adele-server
journalctl -u adele-server -f
```

---

## 项目结构

```
.
├── api/proto/          # Protobuf 定义
├── client/             # 客户端库
├── server/             # 服务端库
├── examples/           # 示例程序
│   ├── server/         # 服务端入口
│   └── client/         # 客户端入口
└── internal/proto/     # 生成的 protobuf 代码
```

---

## 安全建议

1. **限制 gRPC 端口**：安全组中只开放 50051 给你的本地 IP
2. **防火墙**：使用 `iptables` 或 `ufw` 进一步限制
3. **HTTPS**：如需 HTTPS，在 CVM 上部署 Nginx 反向代理

---

## License

MIT

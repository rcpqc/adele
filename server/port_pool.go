package server

import (
	"sync"
)

// PortPool 端口池
type PortPool struct {
	mu     sync.Mutex
	start  int
	end    int
	used   map[int]bool
}

// NewPortPool 创建端口池
func NewPortPool(start, end int) *PortPool {
	return &PortPool{
		start: start,
		end:   end,
		used:  make(map[int]bool),
	}
}

// Allocate 分配一个端口
func (p *PortPool) Allocate() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for port := p.start; port <= p.end; port++ {
		if !p.used[port] {
			p.used[port] = true
			return port, nil
		}
	}
	return 0, ErrPortExhausted
}

// Release 释放端口
func (p *PortPool) Release(port int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.used, port)
}

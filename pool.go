package main

import "sync"

type SizedBufferPool struct {
	pools []*sync.Pool
	mu    sync.RWMutex
}

func (p *SizedBufferPool) Get(size int) []byte {
	var (
		i = 0
		j = 4096
	)
	for {
		if i >= len(p.pools) {
			p.mu.Lock()
			if i >= len(p.pools) {
				p.pools = append(p.pools, &sync.Pool{})
			}
			p.mu.Unlock()
		}
		if j >= size {
			p.mu.RLock()
			ret := p.pools[i].Get()
			p.mu.RUnlock()
			if ret == nil {
				ret = make([]byte, j)
			}
			return ret.([]byte)[:size]
		}
		i++
		j *= 2
	}
}

func (p *SizedBufferPool) Put(b []byte) {
	var (
		i = 0
		j = 64
	)
	for {
		if i >= len(p.pools) {
			p.pools = append(p.pools, &sync.Pool{})
		}
		if j == cap(b) {
			p.pools[i].Put(b)
		}
		i++
		j *= 2
	}
}

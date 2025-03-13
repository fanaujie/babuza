package http

import (
	"net/url"
	"sync"
)

type UrlPool struct {
	pool      sync.Pool
	enableTls bool
}

func NewUrlPool(enableTls bool) *UrlPool {
	return &UrlPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &url.URL{}
			},
		},
		enableTls: enableTls,
	}
}

func (up *UrlPool) Acquire() *url.URL {
	u := up.pool.Get().(*url.URL)
	if up.enableTls {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	return u
}

func (up *UrlPool) Release(u *url.URL) {
	*u = url.URL{}
	up.pool.Put(u)
}

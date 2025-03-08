package http

import (
	"github.com/fanaujie/babuza/ibabuza"
	"net/http"
)

func NewClient(cfg ibabuza.TLSConfig, options Options) (*http.Client, error) {
	var roundTrip http.RoundTripper

	dialCtx, err := dialContext(cfg, options)
	if err != nil {
		return nil, err
	}

	if cfg.EnableTLS == false {
		roundTrip = &http.Transport{
			DialContext: dialCtx,
		}
	} else {
		roundTrip = &http.Transport{
			DialTLSContext: dialCtx,
		}
	}
	//TODO: reuse http.Client, snapshot need new connection
	return &http.Client{
		Transport: roundTrip,
	}, nil
}

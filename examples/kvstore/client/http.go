// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
)

func newHttpClient(enableTLS bool) *http.Client {
	var roundTrip http.RoundTripper

	if enableTLS == false {
		roundTrip = &http.Transport{
			DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		}
	} else {
		roundTrip = &http.Transport{
			DialTLSContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
				return tls.Dial(network, addr, &tls.Config{InsecureSkipVerify: true})
			},
		}
	}
	return &http.Client{
		Transport: roundTrip,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

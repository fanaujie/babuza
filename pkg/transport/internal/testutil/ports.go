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

package testutil

import (
	"net"
	"testing"
)

func FreeTCPAddr(t testing.TB, host string) string {
	t.Helper()
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatalf("listen on free TCP port: %v", err)
	}
	addr := listener.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	if closeErr := listener.Close(); closeErr != nil {
		t.Fatalf("close free TCP port listener: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("parse free TCP address %q: %v", addr, err)
	}
	return net.JoinHostPort(host, port)
}

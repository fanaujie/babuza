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

package ibabuza

type ProxyConfig struct {
	Id                uint64
	InAddr            string
	OutAddr           string
	InListenTLSConfig TLSConfig
	OutDialTLSConfig  TLSConfig
}

type ProxyNetwork interface {
	AddProxy(config ProxyConfig) error
	DeleteProxy(proxyId uint64) error
	ConnectProxy(proxyId uint64) error
	DisconnectProxy(proxyId uint64) error
	SetPartition(proxyIds []uint64) error
	IsProxyConnected(proxyId uint64) bool
	TeardownNetwork() error
	ConnectProxiesIds() []uint64
	DisconnectProxiesIds() []uint64
}

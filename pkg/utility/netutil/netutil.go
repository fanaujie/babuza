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


package netutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/fanaujie/babuza/ibabuza"
	"net"
	"os"
	"strconv"
	"time"
)

//
//var bootstrapLookupAddr = defaultLookUpAddr
//
//func defaultLookUpAddr(ctx context.Context, host string) (*net.IPAddr, error) {
//	r, err := net.DefaultResolver.LookupIPAddr(ctx, host)
//	if err != nil {
//		return nil, err
//	}
//	return &r[0], nil
//}
//
//func ResolveTcpAddr(addr string) (string, error) {
//	host, strPort, err := net.SplitHostPort(addr)
//	if err != nil || len(host) == 0 || len(strPort) == 0 {
//		return "", fmt.Errorf("invalid tcp addr(%s)", addr)
//	}
//
//	//port, err := net.LookupPort("tcp", strPort)
//	port, err := strconv.Atoi(strPort)
//	if err != nil {
//		return "", fmt.Errorf("invalid tcp address(%s)", addr)
//	}
//	if "localhost" == host || net.ParseIP(host) != nil {
//		return addr, nil
//	}
//
//	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
//	defer cancel()
//	for ctx.Err() == nil {
//		ipAddr, err := bootstrapLookupAddr(ctx, host)
//		if err == nil {
//			a := net.TCPAddr{
//				IP:   ipAddr.IP,
//				Port: port,
//				Zone: ipAddr.Zone,
//			}
//			return a.String(), nil
//		}
//		select {
//		case <-ctx.Done():
//			return "", fmt.Errorf("failed to resolver tcp address %s err=(%s)", addr, ctx.Err())
//		case <-time.After(time.Second):
//		}
//	}
//	return "", fmt.Errorf("failed to resolver tcp address %s err=(%s)", addr, ctx.Err())
//}

func IsValidAddress(addr string) bool {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return true
	}
	// check if host is a valid domain name
	if len(host) == 0 || len(host) > 253 || host[0] == '.' || host[len(host)-1] == '.' {
		return false
	}
	return true
}

func GetServerTlsConfig(tc ibabuza.TLSConfig) (*tls.Config, error) {
	if !tc.EnableTLS {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(tc.TLSCert, tc.TLSKey)
	if err != nil {
		return nil, err
	}
	if !tc.MutualTLS {
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}
	certBytes, err := os.ReadFile(tc.TLSRootCA)
	if err != nil {
		return nil, err
	}
	clientCertPool := x509.NewCertPool()
	ok := clientCertPool.AppendCertsFromPEM(certBytes)
	if !ok {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCertPool,
	}, nil
}

func GetClientTlsConfig(tc ibabuza.TLSConfig) (*tls.Config, error) {
	if !tc.EnableTLS {
		return nil, nil
	}
	certBytes, err := os.ReadFile(tc.TLSRootCA)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(certBytes)
	if !ok {
		return nil, err
	}
	if !tc.MutualTLS {
		return &tls.Config{
			RootCAs: certPool,
		}, nil
	}
	cert, err := tls.LoadX509KeyPair(tc.TLSCert, tc.TLSKey)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
	}, nil
}

func TcpDial(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
	tlsCfg, err := GetClientTlsConfig(tlsConfig)
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return net.Dial("tcp", endpoint)
	}
	return tls.Dial("tcp", endpoint, tlsCfg)
}

func TcpDialTimeout(tlsConfig ibabuza.TLSConfig, endpoint string, timeout time.Duration) (net.Conn, error) {
	tlsCfg, err := GetClientTlsConfig(tlsConfig)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: timeout}
	if tlsCfg == nil {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return dialer.DialContext(ctx, "tcp", endpoint)
	}
	return tls.DialWithDialer(dialer, "tcp", endpoint, tlsCfg)
}

func TcpListen(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Listener, error) {
	tlsCfg, err := GetServerTlsConfig(tlsConfig)
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return net.Listen("tcp", endpoint)
	}
	return tls.Listen("tcp", endpoint, tlsCfg)
}

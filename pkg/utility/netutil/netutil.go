package netutil

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/fanaujie/babuza/ibabuza"
	"io/ioutil"
	"net"
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
//func ValidateTcpAddr(addr string, validateIpFormat bool) bool {
//	peerHost, peerPort, err := net.SplitHostPort(addr)
//	if err != nil || len(peerHost) == 0 || len(peerPort) == 0 {
//		return false
//	}
//	//_ , err = net.LookupPort("tcp",peerPort)
//	//if err != nil {
//	//	return false
//	//}
//	if validateIpFormat {
//		return net.ParseIP(peerHost) != nil
//	}
//	return true
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
	certBytes, err := ioutil.ReadFile(tc.TLSRootCA)
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
	certBytes, err := ioutil.ReadFile(tc.TLSRootCA)
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

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

//
//func TestValidateAddr(t *testing.T) {
//	type testCase struct {
//		addr     string
//		ipFormat bool
//		success  bool
//	}
//
//	c := []testCase{
//		{
//			addr: "", ipFormat: false, success: false,
//		},
//		{
//			addr: ":80", ipFormat: false, success: false,
//		},
//		{
//			addr: "www.google.com", ipFormat: false, success: false,
//		},
//		{
//			addr: "www.google.com:80", ipFormat: false, success: true,
//		},
//		{
//			addr: "1.:80", ipFormat: true, success: false,
//		},
//		{
//			addr: "1.2:80", ipFormat: true, success: false,
//		},
//		{
//			addr: "1.2.3:80", ipFormat: true, success: false,
//		},
//		{
//			addr: "1.2.3.4", ipFormat: true, success: false,
//		},
//		{
//			addr: "1.2.3.4:99999", ipFormat: true, success: false,
//		},
//		{
//			addr: "1.2.3.4:-1", ipFormat: true, success: false,
//		},
//		{
//			addr: "1.2.3.4:8080", ipFormat: true, success: true,
//		},
//		{
//			addr: "[fe80:1ff:fe23:4567::]:80", ipFormat: true, success: true,
//		},
//	}
//
//	for _, v := range c {
//		if ValidateTcpAddr(v.addr, v.ipFormat) {
//			if v.success == false {
//				t.Fatalf("test case = %v", v)
//			}
//		} else {
//			if v.success == true {
//				t.Fatalf("test case = %v", v)
//			}
//		}
//	}
//}
//
//func TestResolveAddr(t *testing.T) {
//	defer func() {
//		bootstrapLookupAddr = defaultLookUpAddr
//	}()
//	dnsMap := map[string]string{
//		"one.com": "1.1.1.1",
//	}
//	bootstrapLookupAddr = func(ctx context.Context, host string) (*net.IPAddr, error) {
//		addr, ok := dnsMap[host]
//		if !ok {
//			return nil, errors.New("failed to resolve host")
//		}
//		return &net.IPAddr{IP: net.ParseIP(addr), Zone: ""}, nil
//	}
//	type testCase struct {
//		addr        string
//		resolveAddr string
//	}
//	c := []testCase{
//		//{
//		//	addr: "localhost:80",resolveAddr: "localhost:80",
//		//},
//		{
//			addr: "http://one.com:80", resolveAddr: "1.1.1.1:80",
//		},
//		//{
//		//	addr: "1.1.1.1:80", resolveAddr: "1.1.1.1:80",
//		//},
//		//{
//		//	addr: "two.com:80", resolveAddr: "",
//		//},
//	}
//	for _, v := range c {
//		r, _ := ResolveTcpAddr(v.addr)
//		if r != v.resolveAddr {
//			t.Fatalf("test case = %v", v)
//		}
//	}
//}

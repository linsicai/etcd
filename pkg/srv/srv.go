// Copyright 2015 The etcd Authors
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

// Package srv looks up DNS SRV records.
package srv

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"go.etcd.io/etcd/pkg/types"
)

var (
	// indirection for testing
	lookupSRV      = net.LookupSRV // net.DefaultResolver.LookupSRV when ctxs don't conflict
	// LookupSRV
	// 根据service.proto.name 去找dns 找地址
	// 非标准模式，设置service、proto 为空，直接根据名字找地址

	resolveTCPAddr = net.ResolveTCPAddr
	// 根据地址找ip port
)

// GetCluster gets the cluster information via DNS discovery.
// Also sees each entry as a separate instance.
// 已知接入url
// 去dns 根据scheme、service、name 找地址，然后去接入url 关联上名称
func GetCluster(serviceScheme, service, name, dns string, apurls types.URLs) ([]string, error) {
	tempName := int(0)
	tcp2ap := make(map[string]url.URL)

    // 从接入url，解析出ip port
	// First, resolve the apurls
	for _, url := range apurls {
		tcpAddr, err := resolveTCPAddr("tcp", url.Host)
		if err != nil {
			return nil, err
		}

		tcp2ap[tcpAddr.String()] = url
	}

	stringParts := []string{}
	updateNodeMap := func(service, scheme string) error {
	    // 去dns 查找服务，返回cname、地址、错误
		_, addrs, err := lookupSRV(service, "tcp", dns)
		if err != nil {
			return err
		}

		for _, srv := range addrs {
		    // 计算出host
			port := fmt.Sprintf("%d", srv.Port)
			host := net.JoinHostPort(srv.Target, port)

            // 解析host
			tcpAddr, terr := resolveTCPAddr("tcp", host)
			if terr != nil {
				err = terr
				continue
			}

            // 判断地址重复
			n := ""
			url, ok := tcp2ap[tcpAddr.String()]
			if ok {
				n = name
			}
			if n == "" {
				n = fmt.Sprintf("%d", tempName)
				tempName++
			}

            // 拼kv，顺带判断scheme
			// SRV records have a trailing dot but URL shouldn't.
			shortHost := strings.TrimSuffix(srv.Target, ".")
			urlHost := net.JoinHostPort(shortHost, port)
			if ok && url.Scheme != scheme {
				err = fmt.Errorf("bootstrap at %s from DNS for %s has scheme mismatch with expected peer %s", scheme+"://"+urlHost, service, url.String())
			} else {
				stringParts = append(stringParts, fmt.Sprintf("%s=%s://%s", n, scheme, urlHost))
			}
		}

        // 返回最后的错误码
		if len(stringParts) == 0 {
			return err
		}

        // 成功
		return nil
	}

    // 去dns 更新节点map，根据服务名和scheme
	err := updateNodeMap(service, serviceScheme)
	if err != nil {
		return nil, fmt.Errorf("error querying DNS SRV records for _%s %s", service, err)
	}

	return stringParts, nil
}

type SRVClients struct {
	Endpoints []string
	SRVs      []*net.SRV
}

// GetClient looks up the client endpoints for a service and domain.
func GetClient(service, domain string, serviceName string) (*SRVClients, error) {
	var urls []*url.URL
	var srvs []*net.SRV

	updateURLs := func(service, scheme string) error {
	    // dns 查找
		_, addrs, err := lookupSRV(service, "tcp", domain)
		if err != nil {
			return err
		}

        // 地址转url
		for _, srv := range addrs {
			urls = append(urls, &url.URL{
				Scheme: scheme,
				Host:   net.JoinHostPort(srv.Target, fmt.Sprintf("%d", srv.Port)),
			})
		}

		srvs = append(srvs, addrs...)
		return nil
	}

    // 双通道
	errHTTPS := updateURLs(GetSRVService(service, serviceName, "https"), "https")
	errHTTP := updateURLs(GetSRVService(service, serviceName, "http"), "http")

	if errHTTPS != nil && errHTTP != nil {
		return nil, fmt.Errorf("dns lookup errors: %s and %s", errHTTPS, errHTTP)
	}

    // 输出转换
	endpoints := make([]string, len(urls))
	for i := range urls {
		endpoints[i] = urls[i].String()
	}
	return &SRVClients{Endpoints: endpoints, SRVs: srvs}, nil
}

// 服务-服务名
// 服务-ssl-服务名
// GetSRVService generates a SRV service including an optional suffix.
func GetSRVService(service, serviceName string, scheme string) (SRVService string) {
	if scheme == "https" {
		service = fmt.Sprintf("%s-ssl", service)
	}

	if serviceName != "" {
		return fmt.Sprintf("%s-%s", service, serviceName)
	}

	return service
}

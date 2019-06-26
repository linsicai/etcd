// Copyright 2017 The etcd Authors
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

// Package yaml handles yaml-formatted clientv3 configuration data.
package yaml

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/ghodss/yaml"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/tlsutil"
)

type yamlConfig struct {
    // 客户端配置
	clientv3.Config

    // 不安全的传输
	InsecureTransport     bool   `json:"insecure-transport"`

    // 跳过tls 验证
	InsecureSkipTLSVerify bool   `json:"insecure-skip-tls-verify"`
	// 证书文件
	Certfile              string `json:"cert-file"`
	// key 文件
	Keyfile               string `json:"key-file"`
	// 可信ca 文件
	TrustedCAfile         string `json:"trusted-ca-file"`

	// CAfile is being deprecated. Use 'TrustedCAfile' instead.
	// TODO: deprecate this in v4
	CAfile string `json:"ca-file"`
}

// NewConfig creates a new clientv3.Config from a yaml file.
func NewConfig(fpath string) (*clientv3.Config, error) {
    // 读取文件
	b, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, err
	}

	yc := &yamlConfig{}

    // 反序列化
	err = yaml.Unmarshal(b, yc)
	if err != nil {
		return nil, err
	}

    // 不加密
	if yc.InsecureTransport {
		return &yc.Config, nil
	}

	var (
		cert *tls.Certificate
		cp   *x509.CertPool
	)

    // 读取证书
	if yc.Certfile != "" && yc.Keyfile != "" {
		cert, err = tlsutil.NewCert(yc.Certfile, yc.Keyfile, nil)
		if err != nil {
			return nil, err
		}
	}

    // 读取ca
	if yc.TrustedCAfile != "" {
		cp, err = tlsutil.NewCertPool([]string{yc.TrustedCAfile})
		if err != nil {
			return nil, err
		}
	}

    // 设置tls 配置
	tlscfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: yc.InsecureSkipTLSVerify,
		RootCAs:            cp,
	}
	if cert != nil {
		tlscfg.Certificates = []tls.Certificate{*cert}
	}
	yc.Config.TLS = tlscfg

	return &yc.Config, nil
}

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

package httpproxy

import (
	"math/rand"
	"net/url"
	"sync"
	"time"
)

// defaultRefreshInterval is the default proxyRefreshIntervalMs value
// as in etcdmain/config.go.
// 默认刷新间隔，30秒
const defaultRefreshInterval = 30000 * time.Millisecond

var once sync.Once

func init() {
	// 初始化随机数
	rand.Seed(time.Now().UnixNano())
}

func newDirector(urlsFunc GetProxyURLs, failureWait time.Duration, refreshInterval time.Duration) *director {
	// 创建
	d := &director{
		uf:          urlsFunc,
		failureWait: failureWait,
	}

	// 更新
	d.refresh()

	// 异步做如下
	go func() {
		// In order to prevent missing proxy endpoints in the first try:
		// when given refresh interval of defaultRefreshInterval or greater
		// and whenever there is no available proxy endpoints,
		// give 1-second refreshInterval.
		for {
			// 取节点
			es := d.endpoints()

			// 没节点时刷新快一点
			ri := refreshInterval
			if ri >= defaultRefreshInterval {
				if len(es) == 0 {
					ri = time.Second
				}
			}

			if len(es) > 0 {
				// 只做一次
				once.Do(func() {
					var sl []string

					// 获取url，做打印
					for _, e := range es {
						sl = append(sl, e.URL.String())
					}
					plog.Infof("endpoints found %q", sl)
				})
			}

			// 休眠
			time.Sleep(ri)

			// 刷新
			d.refresh()
		}
	}()

	return d
}

type director struct {
	// 锁
	sync.Mutex

	// 节点列表
	ep []*endpoint

	// 获取代理url 函数
	uf GetProxyURLs

	// 等待时间
	failureWait time.Duration
}

func (d *director) refresh() {
	// 获取代理url
	urls := d.uf()

	// 加锁
	d.Lock()
	defer d.Unlock()

	// 遍历url，得ep
	var endpoints []*endpoint
	for _, u := range urls {
		uu, err := url.Parse(u)
		if err != nil {
			plog.Printf("upstream URL invalid: %v", err)
			continue
		}
		endpoints = append(endpoints, newEndpoint(*uu, d.failureWait))
	}

	// 随机打乱ep列表
	// shuffle array to avoid connections being "stuck" to a single endpoint
	for i := range endpoints {
		j := rand.Intn(i + 1)
		endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	}

	d.ep = endpoints
}

func (d *director) endpoints() []*endpoint {
	// 加锁
	d.Lock()
	defer d.Unlock()

	// 只取活跃的
	filtered := make([]*endpoint, 0)
	for _, ep := range d.ep {
		if ep.Available {
			filtered = append(filtered, ep)
		}
	}

	return filtered
}

func newEndpoint(u url.URL, failureWait time.Duration) *endpoint {
	// 拼ep
	ep := endpoint{
		URL:       u,
		Available: true,
		failFunc:  timedUnavailabilityFunc(failureWait),
	}

	return &ep
}

// ep
type endpoint struct {
	// 锁
	sync.Mutex

	// url
	URL url.URL

	// 可用状态
	Available bool

	// 错误处理函数
	failFunc func(ep *endpoint)
}

func (ep *endpoint) Failed() {
	// 加锁
	ep.Lock()
	if !ep.Available {
		ep.Unlock()
		return
	}

	// 设置状态后解锁
	ep.Available = false
	ep.Unlock()

	// 日志
	plog.Printf("marked endpoint %s unavailable", ep.URL.String())
	if ep.failFunc == nil {
		plog.Printf("no failFunc defined, endpoint %s will be unavailable forever.", ep.URL.String())
		return
	}

	// 调用失败处理函数
	ep.failFunc(ep)
}

// 自动激活
func timedUnavailabilityFunc(wait time.Duration) func(*endpoint) {
	return func(ep *endpoint) {
		// 过段时间后又可用
		time.AfterFunc(wait, func() {
			ep.Available = true
			plog.Printf("marked endpoint %s available, to retest connectivity", ep.URL.String())
		})
	}
}

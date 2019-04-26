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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/pkg/capnslog"
	"go.etcd.io/etcd/etcdserver/api/v2http/httptypes"
)

var (
	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "proxy/httpproxy")

	// Hop-by-hop headers. These are removed when sent to the backend.
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
	// This list of headers borrowed from stdlib httputil.ReverseProxy
	singleHopHeaders = []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te", // canonicalized version of "TE"
		"Trailers",
		"Transfer-Encoding",
		"Upgrade",
	}
)

func removeSingleHopHeaders(hdrs *http.Header) {
    // 移除head
	for _, h := range singleHopHeaders {
		hdrs.Del(h)
	}
}

type reverseProxy struct {
    // 检测
	director  *director

    // 接口
	transport http.RoundTripper
}

func (p *reverseProxy) ServeHTTP(rw http.ResponseWriter, clientreq *http.Request) {
    // 打点
	reportIncomingRequest(clientreq)
	
	// 申请数据
	proxyreq := new(http.Request)
	*proxyreq = *clientreq

    // 开始计时
	startTime := time.Now()

	var (
		proxybody []byte
		err       error
	)

	if clientreq.Body != nil {
	    // 读取请求体
		proxybody, err = ioutil.ReadAll(clientreq.Body)
		if err != nil {
			msg := fmt.Sprintf("failed to read request body: %v", err)
			plog.Println(msg)
			e := httptypes.NewHTTPError(http.StatusInternalServerError, "httpproxy: "+msg)
			if we := e.WriteTo(rw); we != nil {
				plog.Debugf("error writing HTTPError (%v) to %s", we, clientreq.RemoteAddr)
			}
			return
		}
	}

    // 复制http 头
	// deep-copy the headers, as these will be modified below
	proxyreq.Header = make(http.Header)
	copyHeader(proxyreq.Header, clientreq.Header)

    // 归一化请求
	normalizeRequest(proxyreq)

    // 移除不支持head
	removeSingleHopHeaders(&proxyreq.Header)

    // 设置转发信息
	maybeSetForwardedFor(proxyreq)

    // 找一个后端
	endpoints := p.director.endpoints()
	if len(endpoints) == 0 {
		msg := "zero endpoints currently available"
		reportRequestDropped(clientreq, zeroEndpoints)

		// TODO: limit the rate of the error logging.
		plog.Println(msg)
		e := httptypes.NewHTTPError(http.StatusServiceUnavailable, "httpproxy: "+msg)
		if we := e.WriteTo(rw); we != nil {
			plog.Debugf("error writing HTTPError (%v) to %s", we, clientreq.RemoteAddr)
		}
		return
	}

	var requestClosed int32

    // 完成信号量
	completeCh := make(chan bool, 1)

    // 是否支持关闭通知
	closeNotifier, ok := rw.(http.CloseNotifier)

    // 获取上下文
	ctx, cancel := context.WithCancel(context.Background())
	proxyreq = proxyreq.WithContext(ctx)

	defer cancel()
	if ok {
	    // 获取关闭信号量
		closeCh := closeNotifier.CloseNotify()

		go func() {
		    // 异步执行
			select {
			case <-closeCh:
			    // 设置请求关闭，打印日志，做取消
				atomic.StoreInt32(&requestClosed, 1)
				plog.Printf("client %v closed request prematurely", clientreq.RemoteAddr)
				cancel()
			case <-completeCh:
			    // 结束了
			}
		}()

		defer func() {
			completeCh <- true
		}()
	}

	var res *http.Response

	for _, ep := range endpoints {
	    // 做转发
		if proxybody != nil {
			proxyreq.Body = ioutil.NopCloser(bytes.NewBuffer(proxybody))
		}
		redirectRequest(proxyreq, ep.URL)

        // 转发
		res, err = p.transport.RoundTrip(proxyreq)

        // 如果请求关闭
		if atomic.LoadInt32(&requestClosed) == 1 {
			return
		}

		if err != nil {
		    // 转发失败，轮询
			reportRequestDropped(clientreq, failedSendingRequest)
			plog.Printf("failed to direct request to %s: %v", ep.URL.String(), err)
			ep.Failed()
			continue
		}

        // 成功了
		break
	}

    // 响应出错
	if res == nil {
		// TODO: limit the rate of the error logging.
		msg := fmt.Sprintf("unable to get response from %d endpoint(s)", len(endpoints))
		reportRequestDropped(clientreq, failedGettingResponse)
		plog.Println(msg)
		e := httptypes.NewHTTPError(http.StatusBadGateway, "httpproxy: "+msg)
		if we := e.WriteTo(rw); we != nil {
			plog.Debugf("error writing HTTPError (%v) to %s", we, clientreq.RemoteAddr)
		}
		return
	}

	defer res.Body.Close()
	// 打点
	reportRequestHandled(clientreq, res, startTime)
	// 设置header
	removeSingleHopHeaders(&res.Header)
	copyHeader(rw.Header(), res.Header)

    // 写状态
	rw.WriteHeader(res.StatusCode)
	// 写回报体
	io.Copy(rw, res.Body)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func redirectRequest(req *http.Request, loc url.URL) {
	req.URL.Scheme = loc.Scheme
	req.URL.Host = loc.Host
}

func normalizeRequest(req *http.Request) {
	req.Proto = "HTTP/1.1"
	req.ProtoMajor = 1
	req.ProtoMinor = 1
	req.Close = false
}

// 增加一个ip
func maybeSetForwardedFor(req *http.Request) {
	clientIP, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return
	}

	// If we aren't the first proxy retain prior
	// X-Forwarded-For information as a comma+space
	// separated list and fold multiple headers into one.
	if prior, ok := req.Header["X-Forwarded-For"]; ok {
		clientIP = strings.Join(prior, ", ") + ", " + clientIP
	}
	req.Header.Set("X-Forwarded-For", clientIP)
}

// Copyright 2016 The etcd Authors
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

package tcpproxy

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

var plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "proxy/tcpproxy")

type remote struct {
	// 锁
	mu sync.Mutex

	// 服务
	srv *net.SRV

	// 地址
	addr string

	// 是否失效
	inactive bool
}

// 设置失效
func (r *remote) inactivate() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.inactive = true
}

func (r *remote) tryReactivate() error {
	// ping 地址
	conn, err := net.Dial("tcp", r.addr)
	if err != nil {
		return err
	}
	conn.Close()

	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	// 设置有效
	r.inactive = false
	return nil
}

// 看是否有效
func (r *remote) isActive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return !r.inactive
}

type TCPProxy struct {
	// 日志
	Logger *zap.Logger

	// 监听者
	Listener net.Listener

	// 后端配置
	Endpoints []*net.SRV

	// 监控间隔
	MonitorInterval time.Duration

	// 推出信号量
	donec chan struct{}

	// 锁
	mu sync.Mutex // guards the following fields

	// 后台
	remotes []*remote

	// 轮询用
	pickCount int // for round robin
}

func (tp *TCPProxy) Run() error {
	// 加载配置
	tp.donec = make(chan struct{})
	if tp.MonitorInterval == 0 {
		tp.MonitorInterval = 5 * time.Minute
	}
	for _, srv := range tp.Endpoints {
		addr := fmt.Sprintf("%s:%d", srv.Target, srv.Port)
		tp.remotes = append(tp.remotes, &remote{srv: srv, addr: addr})
	}

	// 打印日志
	eps := []string{}
	for _, ep := range tp.Endpoints {
		eps = append(eps, fmt.Sprintf("%s:%d", ep.Target, ep.Port))
	}
	if tp.Logger != nil {
		tp.Logger.Info("ready to proxy client requests", zap.Strings("endpoints", eps))
	} else {
		plog.Printf("ready to proxy client requests to %+v", eps)
	}

	// 异步监控
	go tp.runMonitor()

	for {
		// 监听
		in, err := tp.Listener.Accept()
		if err != nil {
			return err
		}

		// 服务
		go tp.serve(in)
	}
}

func (tp *TCPProxy) pick() *remote {
	var weighted []*remote
	var unweighted []*remote

	bestPr := uint16(65535)
	w := 0
	// find best priority class
	for _, r := range tp.remotes {
		switch {
		case !r.isActive():
		case r.srv.Priority < bestPr:
			bestPr = r.srv.Priority
			w = 0
			weighted = nil
			unweighted = []*remote{r}
			fallthrough
		case r.srv.Priority == bestPr:
			if r.srv.Weight > 0 {
				weighted = append(weighted, r)
				w += int(r.srv.Weight)
			} else {
				unweighted = append(unweighted, r)
			}
		}
	}
	if weighted != nil {
		if len(unweighted) > 0 && rand.Intn(100) == 1 {
			// In the presence of records containing weights greater
			// than 0, records with weight 0 should have a very small
			// chance of being selected.
			r := unweighted[tp.pickCount%len(unweighted)]
			tp.pickCount++
			return r
		}
		// choose a uniform random number between 0 and the sum computed
		// (inclusive), and select the RR whose running sum value is the
		// first in the selected order
		choose := rand.Intn(w)
		for i := 0; i < len(weighted); i++ {
			choose -= int(weighted[i].srv.Weight)
			if choose <= 0 {
				return weighted[i]
			}
		}
	}
	if unweighted != nil {
		for i := 0; i < len(tp.remotes); i++ {
			picked := tp.remotes[tp.pickCount%len(tp.remotes)]
			tp.pickCount++
			if picked.isActive() {
				return picked
			}
		}
	}
	return nil
}

func (tp *TCPProxy) serve(in net.Conn) {
	var (
		err error
		out net.Conn
	)

	for {
		// 选一个后端
		tp.mu.Lock()
		remote := tp.pick()
		tp.mu.Unlock()
		if remote == nil {
			// 全部死了
			break
		}

		// TODO: add timeout
		out, err = net.Dial("tcp", remote.addr)
		if err == nil {
			// 选中可用
			break
		}

		// 不可用，干掉它
		remote.inactivate()
		if tp.Logger != nil {
			tp.Logger.Warn("deactivated endpoint", zap.String("address", remote.addr), zap.Duration("interval", tp.MonitorInterval), zap.Error(err))
		} else {
			plog.Warningf("deactivated endpoint [%s] due to %v for %v", remote.addr, err, tp.MonitorInterval)
		}
	}

	if out == nil {
		// 无可用后端
		in.Close()
		return
	}

	// 转发
	go func() {
		io.Copy(in, out)
		in.Close()
		out.Close()
	}()

	io.Copy(out, in)
	out.Close()
	in.Close()
}

func (tp *TCPProxy) runMonitor() {
	for {
		select {
		case <-time.After(tp.MonitorInterval):
			// 定期事件
			tp.mu.Lock()
			for _, rem := range tp.remotes {
				if rem.isActive() {
					// 活者
					continue
				}

				go func(r *remote) {
					// 尝试激活
					if err := r.tryReactivate(); err != nil {
						if tp.Logger != nil {
							tp.Logger.Warn("failed to activate endpoint (stay inactive for another interval)", zap.String("address", r.addr), zap.Duration("interval", tp.MonitorInterval), zap.Error(err))
						} else {
							plog.Warningf("failed to activate endpoint [%s] due to %v (stay inactive for another %v)", r.addr, err, tp.MonitorInterval)
						}
					} else {
						if tp.Logger != nil {
							tp.Logger.Info("activated", zap.String("address", r.addr))
						} else {
							plog.Printf("activated %s", r.addr)
						}
					}
				}(rem)
			}
			tp.mu.Unlock()
		case <-tp.donec:
			// 结束
			return
		}
	}
}

func (tp *TCPProxy) Stop() {
	// graceful shutdown?
	// shutdown current connections?
	// 关闭监听者
	tp.Listener.Close()

	// 通知结束
	close(tp.donec)
}

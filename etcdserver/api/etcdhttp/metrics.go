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

package etcdhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/raft"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	pathMetrics = "/metrics"

	PathHealth = "/health"
)

// HandleMetricsHealth registers metrics and health handlers.
func HandleMetricsHealth(mux *http.ServeMux, srv etcdserver.ServerV2) {
    // 注册http 响应函数
	mux.Handle(pathMetrics, promhttp.Handler())
	mux.Handle(PathHealth, NewHealthHandler(func() Health { return checkHealth(srv) }))
}

// HandlePrometheus registers prometheus handler on '/metrics'.
func HandlePrometheus(mux *http.ServeMux) {
	mux.Handle(pathMetrics, promhttp.Handler())
}

// NewHealthHandler handles '/health' requests.
func NewHealthHandler(hfunc func() Health) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 方法校验
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		// 运行函数
		h := hfunc()

		// 序列化
		d, _ := json.Marshal(h)
		if h.Health != "true" {
			// 告警
			http.Error(w, string(d), http.StatusServiceUnavailable)
			return
		}

		// 回包
		w.WriteHeader(http.StatusOK)
		w.Write(d)
	}
}

var (
	// 健康度统计
	healthSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "health_success",
		Help:      "The total number of successful health checks",
	})
	healthFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "health_failures",
		Help:      "The total number of failed health checks",
	})
)

func init() {
    // 初始化统计
	prometheus.MustRegister(healthSuccess)
	prometheus.MustRegister(healthFailed)
}

// Health defines etcd server health status.
// TODO: remove manual parsing in etcdctl cluster-health
type Health struct {
	Health string `json:"health"`
}

// TODO: server NOSPACE, etcdserver.ErrNoLeader in health API

func checkHealth(srv etcdserver.ServerV2) Health {
	// 默认健康
	h := Health{Health: "true"}

	// 有告警，不健康
	as := srv.Alarms()
	if len(as) > 0 {
		h.Health = "false"
	}

	if h.Health == "true" {
		// 没有领导者，不健康
		if uint64(srv.Leader()) == raft.None {
			h.Health = "false"
		}
	}

	if h.Health == "true" {
		// http 校验
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := srv.Do(ctx, etcdserverpb.Request{Method: "QGET"})
		cancel()
		if err != nil {
			h.Health = "false"
		}
	}

	// 统计打点
	if h.Health == "true" {
		healthSuccess.Inc()
	} else {
		healthFailed.Inc()
	}
	return h
}

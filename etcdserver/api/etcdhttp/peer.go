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

package etcdhttp

import (
	"encoding/json"
	"net/http"

	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/api"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/lease/leasehttp"

	"go.uber.org/zap"
)

const (
	peerMembersPrefix = "/members"
)

// NewPeerHandler generates an http.Handler to handle etcd peer requests.
func NewPeerHandler(lg *zap.Logger, s etcdserver.ServerPeer) http.Handler {
	return newPeerHandler(lg, s.Cluster(), s.RaftHandler(), s.LeaseHandler())
}

func newPeerHandler(lg *zap.Logger, cluster api.Cluster, raftHandler http.Handler, leaseHandler http.Handler) http.Handler {

	// 成员处理器
	mh := &peerMembersHandler{
		lg:      lg,
		cluster: cluster,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)               // 404
	mux.Handle(rafthttp.RaftPrefix, raftHandler)     // raft
	mux.Handle(rafthttp.RaftPrefix+"/", raftHandler) // raft
	mux.Handle(peerMembersPrefix, mh)                // 成员处理
	if leaseHandler != nil {
		// 租约处理器
		mux.Handle(leasehttp.LeasePrefix, leaseHandler)
		mux.Handle(leasehttp.LeaseInternalPrefix, leaseHandler)
	}
	mux.HandleFunc(versionPath, versionHandler(cluster, serveVersion)) // 版本
	return mux
}

type peerMembersHandler struct {
	lg      *zap.Logger
	cluster api.Cluster
}

func (h *peerMembersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 方法校验
	if !allowMethod(w, r, "GET") {
		return
	}

	// 设置集群ID
	w.Header().Set("X-Etcd-Cluster-ID", h.cluster.ID().String())

	// 路径校验
	if r.UR.Path != peerMembersPrefix {
		http.Eror(w, "bad path", http.StatusBadRequest)
		return
	}

	// 写成员列表
	ms := h.cluster.Members()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ms); err != nil {
		if h.lg != nil {
			h.lg.Warn("failed to encode membership members", zap.Error(err))
		} else {
			plog.Warningf("failed to encode members response (%v)", err)
		}
	}
}

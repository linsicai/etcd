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

package etcdserver

import (
	"encoding/json"
	"expvar"
	"log"
	"sort"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/contention"
	"go.etcd.io/etcd/pkg/logutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	// 吞吐量不超过100MB/s，假设rtt为10ms即100/秒，得1MB
	maxSizePerMsg = 1 * 1024 * 1024

	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

// raft 状态
var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
    // 初始化日志
	lcfg := &zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:      "json",
		EncoderConfig: zap.NewProductionEncoderConfig(),

		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	lg, err := logutil.NewRaftLogger(lcfg)
	if err != nil {
		log.Fatalf("cannot create raft logger %v", err)
	}
	raft.SetLogger(lg)

    // 发布公共函数变量
	expvar.Publish("raft.status", expvar.Func(func() interface{} {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()

		return raftStatus()
	}))
}

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot

	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
}

type raftNode struct {
	lg *zap.Logger

	tickMu *sync.Mutex

	// 节点配置
	raftNodeConfig

	// a chan to send/receive snapshot
	// 快照通道
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	// 申请通道
	applyc chan apply

	// a chan to send out readState
	// 读状态通道
	readStateC chan raft.ReadState

	// utility
	ticker *time.Ticker

	// contention detectors for raft heartbeat message
	td *contention.TimeoutDetector

    // 停止结束信号
	stopped chan struct{}
	done    chan struct{}
}

// 节点配置
type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	// 节点已经删除？
	isIDRemoved func(id uint64) bool

    // 节点信息
	raft.Node

    // 存储
	raftStorage *raft.MemoryStorage
	storage     Storage

    // 心跳间隔
	heartbeat   time.Duration // for logging


	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	transport rafthttp.Transporter
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan apply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}

    // 创建定时器
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}

	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
// 启动节点
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		islead := false

		for {
			select {
			case <-r.ticker.C:
			    // 定时器触发
				r.tick()
			case rd := <-r.Ready():
			    // 状态变更
				if rd.SoftState != nil {
				    // 软状态？

					// 检测leader 变更
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					// 监控是否有leader
					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					// 更新leader 与监控
					rh.updateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}

					// 重置状态
					rh.updateLeadership(newLeader)
					r.td.Reset()
				}

				// 有状态可以读，转发或超时或退出
				if len(rd.ReadStates) != 0 {
					select {
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						if r.lg != nil {
							r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
						} else {
							plog.Warningf("timed out sending read state")
						}
					case <-r.stopped:
						return
					}
				}

				// 更新提交索引
				notifyc := make(chan struct{}, 1)
				ap := apply{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot,
					notifyc:  notifyc,
				}
				updateCommittedIndex(&ap, rh)
				select {
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				// leader 要发送消息
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.transport.Send(r.processMessages(rd.Messages))
				}

				// gofail: var raftBeforeSave struct{}
				// 存储硬件状态
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					if r.lg != nil {
						r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
					} else {
						plog.Fatalf("raft save state and entries error: %v", err)
					}
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				// 快照处理
				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					// 存储快照
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						if r.lg != nil {
							r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
						} else {
							plog.Fatalf("raft save snapshot error: %v", err)
						}
					}
					// etcdserver now claim the snapshot has been persisted onto the disk
					notifyc <- struct{}{}

					// gofail: var raftAfterSaveSnap struct{}
					// 发布快照更新
					r.raftStorage.ApplySnapshot(rd.Snapshot)
					if r.lg != nil {
						r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					} else {
						plog.Infof("raft applied incoming snapshot at index %d", rd.Snapshot.Metadata.Index)
					}
					// gofail: var raftAfterApplySnap struct{}
				}

				// 实体累积
				r.raftStorage.Append(rd.Entries)

				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					// 配置变更
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.transport.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				r.Advance()
			case <-r.stopped:
			    // 停止
				return
			}
		}
	}()
}

// 更新提交索引
func updateCommittedIndex(ap *apply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}

	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				if r.lg != nil {
					r.lg.Warn(
						"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
						zap.Duration("heartbeat-interval", r.heartbeat),
						zap.Duration("expected-duration", 2*r.heartbeat),
						zap.Duration("exceeded-duration", exceed),
					)
				} else {
					plog.Warningf("failed to send out heartbeat on time (exceeded the %v timeout for %v)", r.heartbeat, exceed)
					plog.Warningf("server is likely overloaded")
				}
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

// 获取一个申请
func (r *raftNode) apply() chan apply {
	return r.applyc
}

// 发送停止信号，等待结束
func (r *raftNode) stop() {
	r.stopped <- struct{}{}
	<-r.done
}

func (r *raftNode) onStop() {
    // 停自己、停定时器、停传输层
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()

    // 关闭存储
	if err := r.storage.Close(); err != nil {
		if r.lg != nil {
			r.lg.Panic("failed to close Raft storage", zap.Error(err))
		} else {
			plog.Panicf("raft close storage error: %v", err)
		}
	}

    // 发送结束信号
	close(r.done)
}

// for testing
// 停止传输层
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}
// 恢复传输层
func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
// 时间你快点走
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func startNode(cfg ServerConfig, cl *membership.RaftCluster, ids []types.ID) (id types.ID, n raft.Node, s *raft.MemoryStorage, w *wal.WAL) {
	var err error
	member := cl.MemberByName(cfg.Name)
	metadata := pbutil.MustMarshal(
		&pb.Metadata{
			NodeID:    uint64(member.ID),
			ClusterID: uint64(cl.ID()),
		},
	)
	if w, err = wal.Create(cfg.Logger, cfg.WALDir(), metadata); err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Fatal("failed to create WAL", zap.Error(err))
		} else {
			plog.Fatalf("create wal error: %v", err)
		}
	}
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		ctx, err = json.Marshal((*cl).Member(id))
		if err != nil {
			if cfg.Logger != nil {
				cfg.Logger.Panic("failed to marshal member", zap.Error(err))
			} else {
				plog.Panicf("marshal member should never fail: %v", err)
			}
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	id = member.ID
	if cfg.Logger != nil {
		cfg.Logger.Info(
			"starting local member",
			zap.String("local-member-id", id.String()),
			zap.String("cluster-id", cl.ID().String()),
		)
	} else {
		plog.Infof("starting member %s in cluster %s", id, cl.ID())
	}
	s = raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	n = raft.StartNode(c, peers)
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, n, s, w
}

func restartNode(cfg ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap)

	if cfg.Logger != nil {
		cfg.Logger.Info(
			"restarting local member",
			zap.String("cluster-id", cid.String()),
			zap.String("local-member-id", id.String()),
			zap.Uint64("commit-index", st.Commit),
		)
	} else {
		plog.Infof("restarting member %s in cluster %s at commit index %d", id, cid, st.Commit)
	}
	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(id, cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		var err error
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	n := raft.RestartNode(c)
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, cl, n, s, w
}

func restartAsStandaloneNode(cfg ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap)

	// discard the previously uncommitted entries
	for i, ent := range ents {
		if ent.Index > st.Commit {
			if cfg.Logger != nil {
				cfg.Logger.Info(
					"discarding uncommitted WAL entries",
					zap.Uint64("entry-index", ent.Index),
					zap.Uint64("commit-index-from-wal", st.Commit),
					zap.Int("number-of-discarded-entries", len(ents)-i),
				)
			} else {
				plog.Infof("discarding %d uncommitted WAL entries ", len(ents)-i)
			}
			ents = ents[:i]
			break
		}
	}

	// force append the configuration change entries
	toAppEnts := createConfigChangeEnts(
		cfg.Logger,
		getIDs(cfg.Logger, snapshot, ents),
		uint64(id),
		st.Term,
		st.Commit,
	)
	ents = append(ents, toAppEnts...)

	// force commit newly appended entries
	err := w.Save(raftpb.HardState{}, toAppEnts)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Fatal("failed to save hard state and entries", zap.Error(err))
		} else {
			plog.Fatalf("%v", err)
		}
	}
	if len(ents) != 0 {
		st.Commit = ents[len(ents)-1].Index
	}

	if cfg.Logger != nil {
		cfg.Logger.Info(
			"forcing restart member",
			zap.String("cluster-id", cid.String()),
			zap.String("local-member-id", id.String()),
			zap.Uint64("commit-index", st.Commit),
		)
	} else {
		plog.Printf("forcing restart of member %s in cluster %s at commit index %d", id, cid, st.Commit)
	}

	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(id, cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	n := raft.RestartNode(c)
	raftStatus = n.Status
	return id, cl, n, s, w
}

// getIDs returns an ordered set of IDs included in the given snapshot and
// the entries. The given snapshot/entries can contain two kinds of
// ID-related entry:
// - ConfChangeAddNode, in which case the contained ID will be added into the set.
// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
func getIDs(lg *zap.Logger,
            snap *raftpb.Snapshot,
            ents []raftpb.Entry) []uint64 {
	ids := make(map[uint64]bool)

    // 从快照的元信息里面读取节点列表
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Nodes {
			ids[id] = true
		}
	}

	for _, e := range ents {
		if e.Type != raftpb.EntryConfChange {
		    // 仅处理配置变更实体
			continue
		}

        // 反序列化
		var cc raftpb.ConfChange
		pbutil.MustUnmarshal(&cc, e.Data)

		switch cc.Type {
		case raftpb.ConfChangeAddNode:
		    // 增加节点
			ids[cc.NodeID] = true
		case raftpb.ConfChangeRemoveNode:
		    // 删除节点
			delete(ids, cc.NodeID)
		case raftpb.ConfChangeUpdateNode:
		    // 更新节点
			// do nothing
		default:
		    // 异常报错
			if lg != nil {
				lg.Panic("unknown ConfChange Type", zap.String("type", cc.Type.String()))
			} else {
				plog.Panicf("ConfChange Type should be either ConfChangeAddNode or ConfChangeRemoveNode!")
			}
		}
	}

    // 排序后返回
	sids := make(types.Uint64Slice, 0, len(ids))
	for id := range ids {
		sids = append(sids, id)
	}
	sort.Sort(sids)
	return []uint64(sids)
}

// createConfigChangeEnts creates a series of Raft entries (i.e.
// EntryConfChange) to remove the set of given IDs from the cluster. The ID
// `self` is _not_ removed, even if present in the set.
// If `self` is not inside the given ids, it creates a Raft entry to add a
// default member with the given `self`.
func createConfigChangeEnts(lg *zap.Logger,
                            ids []uint64,
                            self uint64,
                            term, index uint64)
                            []raftpb.Entry {
	ents := make([]raftpb.Entry, 0)

	next := index + 1

    // 遍历id
	found := false
	for _, id := range ids {
		if id == self {
		    // 找到自己了
			found = true
			continue
		}

        // 配置变更
		cc := &raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	if !found {
	    // 如果没有自己

		// 节点属性
		m := membership.Member{
			ID:             types.ID(self),
			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
		}
		ctx, err := json.Marshal(m)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to marshal member", zap.Error(err))
			} else {
				plog.Panicf("marshal member should never fail: %v", err)
			}
		}

        // 添加节点
		cc := &raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  self,
			Context: ctx,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
	}

	return ents
}

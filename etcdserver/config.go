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
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/pkg/netutil"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/pkg/types"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ServerConfig holds the configuration of etcd as taken from the command line or discovery.
type ServerConfig struct {
	Name           string // 本节点名称
	DiscoveryURL   string // 发现url
	DiscoveryProxy string // 发现代理
	ClientURLs     types.URLs // 客户端url
	PeerURLs       types.URLs // 集群url？
	DataDir        string // 数据目录
	// DedicatedWALDir config will make the etcd to write the WAL to the WALDir
	// rather than the dataDir/member/wal.
	DedicatedWALDir string // 指定的wal 目录

	SnapshotCount uint64 // 快照数

	// SnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	// WARNING: only change this for tests. Always use "DefaultSnapshotCatchUpEntries"
	SnapshotCatchUpEntries uint64

	MaxSnapFiles uint // 最大快照文件数
	MaxWALFiles  uint // 最大wal 文件数

	// BackendBatchInterval is the maximum time before commit the backend transaction.
	BackendBatchInterval time.Duration // 后端batch 延时
	// BackendBatchLimit is the maximum operations before commit the backend transaction.
	BackendBatchLimit int // 后端batch 限制

	InitialPeerURLsMap  types.URLsMap // 初始化集群
	InitialClusterToken string // 初始化集群token
	NewCluster          bool // 新集群？
	PeerTLSInfo         transport.TLSInfo // 传输层加密选项

    // ？？？
	CORS map[string]struct{}

	// HostWhitelist lists acceptable hostnames from client requests.
	// If server is insecure (no TLS), server only accepts requests
	// whose Host header value exists in this white list.
	// 白名单
	HostWhitelist map[string]struct{}

	TickMs        uint // 每tick 多少毫秒
	ElectionTicks int // 选举tick

	// InitialElectionTickAdvance is true, then local member fast-forwards
	// election ticks to speed up "initial" leader election trigger. This
	// benefits the case of larger election ticks. For instance, cross
	// datacenter deployment may require longer election timeout of 10-second.
	// If true, local node does not need wait up to 10-second. Instead,
	// forwards its election ticks to 8-second, and have only 2-second left
	// before leader election.
	//
	// Major assumptions are that:
	//  - cluster has no active leader thus advancing ticks enables faster
	//    leader election, or
	//  - cluster already has an established leader, and rejoining follower
	//    is likely to receive heartbeats from the leader after tick advance
	//    and before election timeout.
	//
	// However, when network from leader to rejoining follower is congested,
	// and the follower does not receive leader heartbeat within left election
	// ticks, disruptive election has to happen thus affecting cluster
	// availabilities.
	//
	// Disabling this would slow down initial bootstrap process for cross
	// datacenter deployments. Make your own tradeoffs by configuring
	// --initial-election-tick-advance at the cost of slow initial bootstrap.
	//
	// If single-node, it advances ticks regardless.
	//
	// See https://github.com/etcd-io/etcd/issues/9333 for more detail.
	// 初始化选举？
	InitialElectionTickAdvance bool

    // 前端超时？
	BootstrapTimeout time.Duration

    // 自动压缩
	AutoCompactionRetention time.Duration
	AutoCompactionMode      string
	QuotaBackendBytes       int64
	MaxTxnOps               uint

	// MaxRequestBytes is the maximum request size to send over raft.
	// 最大请求字节数
	MaxRequestBytes uint

	StrictReconfigCheck bool

	// ClientCertAuthEnabled is true when cert has been signed by the client CA.
	// 客户端权限校验
	ClientCertAuthEnabled bool

    // 权限
	AuthToken  string
	BcryptCost uint

	// InitialCorruptCheck is true to check data corruption on boot
	// before serving any peer/client traffic.
	InitialCorruptCheck bool
	CorruptCheckTime    time.Duration

	// PreVote is true to enable Raft Pre-Vote.
	// 预投票
	PreVote bool

	// Logger logs server-side operations.
	// If not nil, it disables "capnslog" and uses the given logger.
	// 日志
	Logger *zap.Logger

	// LoggerConfig is server logger configuration for Raft logger.
	// Must be either: "LoggerConfig != nil" or "LoggerCore != nil && LoggerWriteSyncer != nil".
	LoggerConfig *zap.Config
	// LoggerCore is "zapcore.Core" for raft logger.
	// Must be either: "LoggerConfig != nil" or "LoggerCore != nil && LoggerWriteSyncer != nil".
	LoggerCore        zapcore.Core
	LoggerWriteSyncer zapcore.WriteSyncer

    // 调试
	Debug bool

    // 强制新集群
	ForceNewCluster bool

	// LeaseCheckpointInterval time.Duration is the wait duration between lease checkpoints.
	// 租约落盘间隔
	LeaseCheckpointInterval time.Duration

    // 开启grpc 网关
	EnableGRPCGateway bool
}

// VerifyBootstrap sanity-checks the initial config for bootstrap case
// and returns an error for things that should never happen.
// 校验前端？
func (c *ServerConfig) VerifyBootstrap() error {
    // 校验本地成员
	if err := c.hasLocalMember(); err != nil {
		return err
	}

    // 校验建议集群
	if err := c.advertiseMatchesCluster(); err != nil {
		return err
	}

    // 校验对端url
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}

    // 校验集群
	if c.InitialPeerURLsMap.String() == "" && c.DiscoveryURL == "" {
		return fmt.Errorf("initial cluster unset and no discovery URL found")
	}

	return nil
}

// VerifyJoinExisting sanity-checks the initial config for join existing cluster
// case and returns an error for things that should never happen.
// 校验是否可以加入集群
func (c *ServerConfig) VerifyJoinExisting() error {
	// The member has announced its peer urls to the cluster before starting; no need to
	// set the configuration again.
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	if c.DiscoveryURL != "" {
		return fmt.Errorf("discovery URL should not be set when joining existing initial cluster")
	}
	return nil
}

// hasLocalMember checks that the cluster at least contains the local server.
// 找本节点url
func (c *ServerConfig) hasLocalMember() error {
	if urls := c.InitialPeerURLsMap[c.Name]; urls == nil {
		return fmt.Errorf("couldn't find local name %q in the initial cluster configuration", c.Name)
	}

	return nil
}

// advertiseMatchesCluster confirms peer URLs match those in the cluster peer list.
// 校验集群？
func (c *ServerConfig) advertiseMatchesCluster() error {
	urls, apurls := c.InitialPeerURLsMap[c.Name], c.PeerURLs.StringSlice()

    // 排序
	urls.Sort()
	sort.Strings(apurls)

    // 创建上下文
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

    // 两个相等没有问题
	ok, err := netutil.URLStringsEqual(ctx, c.Logger, apurls, urls.StringSlice())
	if ok {
		return nil
	}

    // 构建map
	initMap, apMap := make(map[string]struct{}), make(map[string]struct{})
	for _, url := range c.PeerURLs {
		apMap[url.String()] = struct{}{}
	}
	for _, url := range c.InitialPeerURLsMap[c.Name] {
		initMap[url.String()] = struct{}{}
	}

    // 找init 缺失的
	missing := []string{}
	for url := range initMap {
		if _, ok := apMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
	    // 有缺失，报错
		for i := range missing {
			missing[i] = c.Name + "=" + missing[i]
		}
		mstr := strings.Join(missing, ",")
		apStr := strings.Join(apurls, ",")
		return fmt.Errorf("--initial-cluster has %s but missing from --initial-advertise-peer-urls=%s (%v)", mstr, apStr, err)
	}

    // 找ap 缺失
	for url := range apMap {
		if _, ok := initMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
	    // 有缺失，报错
		mstr := strings.Join(missing, ",")
		umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
		return fmt.Errorf("--initial-advertise-peer-urls has %s but missing from --initial-cluster=%s", mstr, umap.String())
	}

	// resolved URLs from "--initial-advertise-peer-urls" and "--initial-cluster" did not match or failed
	// 默认报错
	apStr := strings.Join(apurls, ",")
	umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
	return fmt.Errorf("failed to resolve %s to match --initial-cluster=%s (%v)", apStr, umap.String(), err)
}

// 成员路径
func (c *ServerConfig) MemberDir() string {
    return filepath.Join(c.DataDir, "member")
}

// wal 路径
func (c *ServerConfig) WALDir() string {
	if c.DedicatedWALDir != "" {
		return c.DedicatedWALDir
	}

	return filepath.Join(c.MemberDir(), "wal")
}

// 快照路径
func (c *ServerConfig) SnapDir() string {
    return filepath.Join(c.MemberDir(), "snap")
}

// 是否需要节点发现
func (c *ServerConfig) ShouldDiscover() bool {
    return c.DiscoveryURL != ""
}

// ReqTimeout returns timeout for request to finish.
// 请求超时：5秒 + 2倍选举耗时
func (c *ServerConfig) ReqTimeout() time.Duration {
	// 5s for queue waiting, computation and disk IO delay
	// + 2 * election timeout for possible leader election
	return 5*time.Second + 2*time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

// 选举超时
func (c *ServerConfig) electionTimeout() time.Duration {
	return time.Duration(c.ElectionTicks*int(c.TickMs)) * time.Millisecond
}

// ping 超时
// 1秒 + tick耗时
func (c *ServerConfig) peerDialTimeout() time.Duration {
	// 1s for queue wait and election timeout
	return time.Second \
	+ time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

// 校验是否有重复url
func checkDuplicateURL(urlsmap types.URLsMap) bool {
	um := make(map[string]bool)

	for _, urls := range urlsmap {
		for _, url := range urls {
			u := url.String()
			if um[u] {
				return true
			}
			um[u] = true
		}
	}

	return false
}

// 默认1秒
func (c *ServerConfig) bootstrapTimeout() time.Duration {
	if c.BootstrapTimeout != 0 {
		return c.BootstrapTimeout
	}

	return time.Second
}

// 后端路径
func (c *ServerConfig) backendPath() string {
    return filepath.Join(c.SnapDir(), "db")
}

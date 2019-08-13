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

/*

Package raft sends and receives messages in the Protocol Buffer format
defined in the raftpb package.
收发消息使用pb协议。

Raft is a protocol with which a cluster of nodes can maintain a replicated state machine.
Raft是一种协议，节点集群可以使用该协议来维护复制的状态机。


The state machine is kept in sync through the use of a replicated log.
状态机的同步，通过复制日志来完成。


For more details on Raft, see "In Search of an Understandable Consensus Algorithm"
(https://ramcloud.stanford.edu/raft.pdf) by Diego Ongaro and John Ousterhout.
A simple example application, _raftexample_, is also available to help illustrate
how to use this package in practice:
https://github.com/etcd-io/etcd/tree/master/contrib/raftexample
介绍参考文献

Usage

The primary object in raft is a Node.
主要对象是raft node

You either start a Node from scratch
using raft.StartNode or start a Node from some initial state using raft.RestartNode.
要么从头开始，调用raft.StartNode
要么基于初始化状态，调用raft.RestartNode

To start a node from scratch:
从新开始启动，建存储，设置配置，启动节点
  storage := raft.NewMemoryStorage()
  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxSizePerMsg:   4096,
    MaxInflightMsgs: 256,
  }
  n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

To restart a node from previous state:
重启节点，基于快照、硬状态、实体恢复存储，设置配置，重启节点
  storage := raft.NewMemoryStorage()

  // recover the in-memory storage from persistent
  // snapshot, state and entries.
  storage.ApplySnapshot(snapshot)
  storage.SetHardState(state)
  storage.Append(entries)

  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxSizePerMsg:   4096,
    MaxInflightMsgs: 256,
  }

  // restart raft without peer information.
  // peer information is already included in the storage.
  n := raft.RestartNode(c)

Now that you are holding onto a Node you have a few responsibilities:
拥有节点，有如下责任

First, you must read from the Node.Ready() channel and process the updates
it contains.
首先，需要从ready 通道中读取与执行更新请求

These steps may be performed in parallel, except as noted in step
以下步骤可能是并行的，除非有特别说明
2.

1. Write HardState, Entries, and Snapshot to persistent storage if they are
not empty.
Note that when writing an Entry with Index i, any
previously-persisted entries with Index >= i must be discarded.
持久化硬状态、实体和快照。
放弃比之前任一实体索引小的实体


2. Send all Messages to the nodes named in the To field.
发送消息给to
It is important that
no messages be sent until the latest HardState has been persisted to disk,
and all Entries written by any previous Ready batch (Messages may be sent while
entries from the same batch are being persisted).
发送消息之前，需要把最近的硬状态、之前的实体序列化。
消息和当前批次的实体可以并行发送

To reduce the I/O latency, an
optimization can be applied to make leader write to disk in parallel with its
followers (as explained at section 10.2.1 in Raft thesis).
为减少io 延时，可以将领导者到追随者的写优化成并行

If any Message has type
MsgSnap, call Node.ReportSnapshot() after it has been sent (these messages may be
large).
如果消息有类型MsgSnap，发送消息完后，节点上报快照

Note: Marshalling messages is not thread-safe; it is important that you
make sure that no new entries are persisted while marshalling.
序列化消息不是线程安全的，需要保证序列化的时候，没有实体在做持久化
The easiest way to achieve this is to serialize the messages directly inside
your main raft loop.
可以将序列化放在主线程中，以保证这个窜行

3. Apply Snapshot (if any) and CommittedEntries to the state machine.
将快照和已提交实体提交到状态机。

If any committed Entry has Type EntryConfChange, call Node.ApplyConfChange()
to apply it to the node.
配置变更实体分发规则。

The configuration change may be cancelled at this point
by setting the NodeID field to zero before calling ApplyConfChange
可以通过将NodeID 置零，来取消配置变更。
ApplyConfChange必须调用，取消决策必须基于状态机，而不是外部观察到的节点健康状态。
(but ApplyConfChange must be called one way or the other, and the decision to cancel
must be based solely on the state machine and not external information such as
the observed health of the node).

4. Call Node.Advance() to signal readiness for the next batch of updates.
调用Node.Advance 表示已经为下一批次做好准备。
This may be done at any time after step 1, although all updates must be processed
in the order they were returned by Ready.
可以在step 1，之后的任意时间调用，但是ready 必须按顺序执行

Second, all persisted log entries must be made available via an
implementation of the Storage interface.
所有持久化的实体需要通过storage 接口来实现

The provided MemoryStorage
type can be used for this (if you repopulate its state upon a
restart), or you can supply your own disk-backed implementation.
可以使用默认的MemoryStorage，如果需要重启后恢复，需要自己实现基于磁盘的。

Third, when you receive a message from another node, pass it to Node.Step:
将其他节点的消息，传递给step
	func recvRaftRPC(ctx context.Context, m raftpb.Message) {
		n.Step(ctx, m)
	}


Finally, you need to call Node.Tick() at regular intervals (probably
via a time.Ticker).
需要定期调用tick

Raft has two important timeouts: heartbeat and the
election timeout.
raft 有两个重要的超时，心跳超时和选举超时。

However, internally to the raft package time is
represented by an abstract "tick".
raft 内部的时间抽象为tick

The total state machine handling loop will look something like this:
状态机大致如下

  for {
    select {
    case <-s.Ticker: // tick
      n.Tick()
    case rd := <-s.Node.Ready():
      // 序列化
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      // 发送消息
      send(rd.Messages)
      // 处理快照
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      // 处理已提交实体
      for _, entry := range rd.CommittedEntries {
        process(entry)
        if entry.Type == raftpb.EntryConfChange {
          // 配置变更
          var cc raftpb.ConfChange
          cc.Unmarshal(entry.Data)
          s.Node.ApplyConfChange(cc)
        }
      }
      // 前进
      s.Node.Advance()
    case <-s.done:
      // 结束
      return
    }
  }

To propose changes to the state machine from your node take your application
data, serialize it into a byte slice and call:
	n.Propose(ctx, data)
修改状态机，序列化data，并调用propose

If the proposal is committed, data will appear in committed entries with type
raftpb.EntryNormal.
如果提议被提交，数据会出现在已提交实体。


There is no guarantee that a proposed command will be
committed; you may have to re-propose after a timeout.
不保证肯定被提交，超时后需要重新提交。

To add or remove a node in a cluster, build ConfChange struct 'cc' and call:
	n.ProposeConfChange(ctx, cc)
加入和离开集群，调用提议集群变更。

After config change is committed, some committed entry with type
raftpb.EntryConfChange will be returned.
一旦配置变更被提交，会返回已提交的配置变更实体

You must apply it to node through:
	var cc raftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)
接着需要反序列化数据，并应用到节点

Note: An ID represents a unique node in a cluster for all time.
ID 表示集群中的唯一节点

A
given ID MUST be used only once even if the old node has been removed.
ID 不能重复使用

This means that for example IP addresses make poor node IDs since they
may be reused.
这意味着ip 不是个好ID，因为它们会被重复利用。

Node IDs must be non-zero.
ID 必须非0

Implementation notes

This implementation is up to date with the final Raft thesis
(https://ramcloud.stanford.edu/~ongaro/thesis.pdf), although our
implementation of the membership change protocol differs somewhat from
that described in chapter 4.

The key invariant that membership changes
happen one node at a time is preserved, but in our implementation the
membership change takes effect when its entry is applied, not when it
is added to the log (so the entry is committed under the old
membership instead of the new).

This is equivalent in terms of safety,
since the old and new configurations are guaranteed to overlap.

To ensure that we do not attempt to commit two membership changes at
once by matching log positions (which would be unsafe since they
should have different quorum requirements), we simply disallow any
proposed membership change while any uncommitted change appears in
the leader's log.

This approach introduces a problem when you try to remove a member
from a two-member cluster: If one of the members dies before the
other one receives the commit of the confchange entry, then the member
cannot be removed any more since the cluster cannot make progress.
For this reason it is highly recommended to use three or more nodes in
every cluster.

MessageType

Package raft sends and receives message in Protocol Buffer format (defined
in raftpb package).

Each state (follower, candidate, leader) implements its
own 'step' method ('stepFollower', 'stepCandidate', 'stepLeader') when
advancing with the given raftpb.Message
每个角色都有step函数

Each step is determined by its raftpb.MessageType.
每个step 做的事情取决于消息类型

Note that every step is checked by one common method
'Step' that safety-checks the terms of node and incoming message to prevent
stale log entries:

    // MsgHup
	'MsgHup' is used for election. 用于选举
	If a node is a follower or candidate, the 'tick' function in 'raft' struct is set as 'tickElection'.
	追随者和候选者使用选举tick
	If a follower or
	candidate has not received any heartbeat before the election timeout, it
	passes 'MsgHup' to its Step method and becomes (or remains) a candidate to
	start a new election.
	如果追随者或候选人在选举超时前没有收到心跳，传递MsgHup给step，然后变成候选人，然后开始选举。

	'MsgBeat' is an internal type that signals the leader to send a heartbeat of
	the 'MsgHeartbeat' type.
	领导者发心跳给追随者。
	If a node is a leader, the 'tick' function in
	the 'raft' struct is set as 'tickHeartbeat', and triggers the leader to
	send periodic 'MsgHeartbeat' messages to its followers.

	'MsgProp' proposes to append data to its log entries. 
	提议消息表示追加消息到日志实体。
	This is a special
	type to redirect proposals to leader.
	Therefore, send method overwrites
	raftpb.Message's term with its HardState's term to avoid attaching its
	local term to 'MsgProp'.
	发送者会改写消息的term，使用硬状态的term
	When 'MsgProp' is passed to the leader's 'Step'
	method, the leader first calls the 'appendEntry' method to append entries
	to its log, and then calls 'bcastAppend' method to send those entries to
	its peers.
	领导者将消息追加到实体列表，然后广播出去
	When passed to candidate, 'MsgProp' is dropped.
	候选者丢弃
	When passed to
	follower, 'MsgProp' is stored in follower's mailbox(msgs) by the send
	method.
	追随者放到收件箱里面，稍后发给领导者
	It is stored with sender's ID and later forwarded to leader by
	rafthttp package.

	'MsgApp' contains log entries to replicate.
	包含要复制的日志条目
	A leader calls bcastAppend,
	which calls sendAppend, which sends soon-to-be-replicated logs in 'MsgApp'
	type.
	领导者广播发送
	When 'MsgApp' is passed to candidate's Step method, candidate reverts
	back to follower, because it indicates that there is a valid leader sending MsgApp' messages.
	候选者退回至追随者，因为有一个有效的领导者。
	Candidate and follower respond to this message in
	'MsgAppResp' type.
	候选者和追随者需要回包

	'MsgAppResp' is response to log replication request('MsgApp').
	When
	'MsgApp' is passed to candidate or follower's Step method, it responds by
	calling 'handleAppendEntries' method, which sends 'MsgAppResp' to raft
	mailbox.
	候选者和追随者回包

	'MsgVote' requests votes for election.
	选举投票
	When a node is a follower or
	candidate and 'MsgHup' is passed to its Step method, then the node calls
	'campaign' method to campaign itself to become a leader. 
	候选者和追随者收到后，开始竞争为领导者。
	Once 'campaign'
	method is called, the node becomes candidate and sends 'MsgVote' to peers
	in cluster to request votes.
	一旦战争开始，节点变成候选者，发送投票给集群的其他节点。
	When passed to leader or candidate's Step
	method and the message's Term is lower than leader's or candidate's,
	'MsgVote' will be rejected ('MsgVoteResp' is returned with Reject true).
	候选者和领导者会拒绝term 小的投票。
	If leader or candidate receives 'MsgVote' with higher term, it will revert
	back to follower.
	领导者和候选者收到更高term 的投票，回退到追随者。
	When 'MsgVote' is passed to follower, it votes for the
	sender only when sender's last term is greater than MsgVote's term or
	sender's last term is equal to MsgVote's term but sender's last committed
	index is greater than or equal to follower's.
	追随者收到后，投票给last term比较大的sender，或者term 相等且索引比较大的。

	'MsgVoteResp' contains responses from voting request. 
	投票回包
	When 'MsgVoteResp' is
	passed to candidate, the candidate calculates how many votes it has won.
	候选者计算他赢了多少票，如果赢得了大多数票，成为领导者，并广播消息。
	If
	it's more than majority (quorum), it becomes leader and calls 'bcastAppend'.
	If candidate receives majority of votes of denials, it reverts back to
	follower.
	如果获得大多数拒绝，变成追随者

	'MsgPreVote' and 'MsgPreVoteResp' are used in an optional two-phase election
	protocol.
	预投票只要用到两阶段选举协议。
	When Config.PreVote is true, a pre-election is carried out first
	(using the same rules as a regular election), and no node increases its term
	number unless the pre-election indicates that the campaigning node would win.
	预投票阶段，结束后再增加term
	This minimizes disruption when a partitioned node rejoins the cluster.
	当分区节点重加入集群时，用来减少中断。

	'MsgSnap' requests to install a snapshot message.
	安装快照消息
	When a node has just
	become a leader or the leader receives 'MsgProp' message, it calls
	'bcastAppend' method, which then calls 'sendAppend' method to each
	follower.
	领导者收到快照后，将广播并发送消息。
	In 'sendAppend', if a leader fails to get term or entries,
	the leader requests snapshot by sending 'MsgSnap' type message.
	如果领导者发送消息时，获取term或实体失败，会再发消息

	'MsgSnapStatus' tells the result of snapshot install message.
	是快照消息的结果
	When a
	follower rejected 'MsgSnap', it indicates the snapshot request with
	'MsgSnap' had failed from network issues which causes the network layer
	to fail to send out snapshots to its followers.
	当拒绝快照，表示没有发送成功
	Then leader considers
	follower's progress as probe.
	领导者会对追随者做探测
	When 'MsgSnap' were not rejected, it
	indicates that the snapshot succeeded and the leader sets follower's
	progress to probe and resumes its log replication.
	当快照没有被拒绝，表示追随者快照成功，领导者可以继续做日志复制。

	'MsgHeartbeat' sends heartbeat from leader.
	心跳是发送给领导者的。
	When 'MsgHeartbeat' is passed
	to candidate and message's term is higher than candidate's, the candidate
	reverts back to follower and updates its committed index from the one in
	this heartbeat.
	候选者收到更高的term，候选者退回到追随者，并更新已提交索引
	And it sends the message to its mailbox.
	有单独的心跳通道
	When
	'MsgHeartbeat' is passed to follower's Step method and message's term is
	higher than follower's, the follower updates its leaderID with the ID
	from the message.
	追随者收到更高的term，更新领导者的id

	'MsgHeartbeatResp' is a response to 'MsgHeartbeat'.
	When 'MsgHeartbeatResp'
	is passed to leader's Step method, the leader knows which follower
	responded.
	追随者给领导者的回包
	And only when the leader's last committed index is greater than
	follower's Match index, the leader runs 'sendAppend` method.
	只当领导者已提交索引大于追随者的match 索引时，领导者才发消息。
	

	'MsgUnreachable' tells that request(message) wasn't delivered.
	指示消息无法投递
	When
	'MsgUnreachable' is passed to leader's Step method, the leader discovers
	that the follower that sent this 'MsgUnreachable' is not reachable, often
	indicating 'MsgApp' is lost.
	追随者发送给领导者，用于指示复制消息丢失。
	When follower's progress state is replicate,
	the leader sets it back to probe.
	领导者将重新探测复制进度。

*/
package raft

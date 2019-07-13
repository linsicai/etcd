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

package raft

import (
	"fmt"

	pb "go.etcd.io/etcd/raft/raftpb"
)

type Status struct {
	ID uint64 // id

	pb.HardState // 硬状态
	SoftState    // 软状态

	Applied  uint64              // 应用ID
	Progress map[uint64]Progress // 过程

	LeadTransferee uint64
}

// 深度复制，获取进度
func getProgressCopy(r *raft) map[uint64]Progress {
	prs := make(map[uint64]Progress)
	for id, p := range r.prs {
		prs[id] = *p
	}

	for id, p := range r.learnerPrs {
		prs[id] = *p
	}

	return prs
}

// 复制状态，不需要进度
func getStatusWithoutProgress(r *raft) Status {
	s := Status{
		ID:             r.id,
		LeadTransferee: r.leadTransferee,
	}

	s.HardState = r.hardState()
	s.SoftState = *r.softState()
	s.Applied = r.raftLog.applied
	return s
}

// getStatus gets a copy of the current raft status.
// 获取状态，如果是leader，需要把进度也复制出来
func getStatus(r *raft) Status {
	s := getStatusWithoutProgress(r)

	if s.RaftState == StateLeader {
		s.Progress = getProgressCopy(r)
	}

	return s
}

// MarshalJSON translates the raft status into JSON.
// TODO: try to simplify this by introducing ID type into raft
// 序列化
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d,"state":%q},`, k, v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

// 序列化
func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		raftLogger.Panicf("unexpected error: %v", err)
	}

	return string(b)
}

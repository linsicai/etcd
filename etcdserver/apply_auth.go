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

package etcdserver

import (
	"sync"

	"go.etcd.io/etcd/auth"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc"
)

type authApplierV3 struct {
	applierV3 // impl

	as     auth.AuthStore // 权限
	lessor lease.Lessor // 租约

	// mu serializes Apply so that user isn't corrupted and so that
	// serialized requests don't leak data from TOCTOU errors
	mu sync.Mutex // 锁

	authInfo auth.AuthInfo // 权限信息
}

// 构建
func newAuthApplierV3(as auth.AuthStore, base applierV3, lessor lease.Lessor) *authApplierV3 {
	return &authApplierV3{applierV3: base, as: as, lessor: lessor}
}

// 应用
func (aa *authApplierV3) Apply(r *pb.InternalRaftRequest) *applyResult {
    // 加锁
	aa.mu.Lock()
	defer aa.mu.Unlock()

    // 记录用户信息
	if r.Header != nil {
		// backward-compatible with pre-3.0 releases when internalRaftRequest
		// does not have header field
		aa.authInfo.Username = r.Header.Username
		aa.authInfo.Revision = r.Header.AuthRevision
	}

    // 校验是否需要管理员权限
	if needAdminPermission(r) {
	    // 校验是否管理员
		if err := aa.as.IsAdminPermitted(&aa.authInfo); err != nil {
			aa.authInfo.Username = ""
			aa.authInfo.Revision = 0
			return &applyResult{err: err}
		}
	}

    // 申请权限
	ret := aa.applierV3.Apply(r)
	aa.authInfo.Username = ""
	aa.authInfo.Revision = 0
	return ret
}

func (aa *authApplierV3) Put(txn mvcc.TxnWrite, r *pb.PutRequest) (*pb.PutResponse, error) {
    // 校验权限
	if err := aa.as.IsPutPermitted(&aa.authInfo, r.Key); err != nil {
		return nil, err
	}

    // 校验租约
	if err := aa.checkLeasePuts(lease.LeaseID(r.Lease)); err != nil {
		// The specified lease is already attached with a key that cannot
		// be written by this user. It means the user cannot revoke the
		// lease so attaching the lease to the newly written key should
		// be forbidden.
		return nil, err
	}

    // 校验老值查询权限
	if r.PrevKv {
		err := aa.as.IsRangePermitted(&aa.authInfo, r.Key, nil)
		if err != nil {
			return nil, err
		}
	}

    // put
	return aa.applierV3.Put(txn, r)
}

func (aa *authApplierV3) Range(txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
    // 校验权限
	if err := aa.as.IsRangePermitted(&aa.authInfo, r.Key, r.RangeEnd); err != nil {
		return nil, err
	}

	return aa.applierV3.Range(txn, r)
}

func (aa *authApplierV3) DeleteRange(txn mvcc.TxnWrite, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
    // 校验删除权限
	if err := aa.as.IsDeleteRangePermitted(&aa.authInfo, r.Key, r.RangeEnd); err != nil {
		return nil, err
	}

    // 校验老值权限
	if r.PrevKv {
		err := aa.as.IsRangePermitted(&aa.authInfo, r.Key, r.RangeEnd)
		if err != nil {
			return nil, err
		}
	}

	return aa.applierV3.DeleteRange(txn, r)
}

// 校验事务权限
func checkTxnReqsPermission(as auth.AuthStore, ai *auth.AuthInfo, reqs []*pb.RequestOp) error 
    // 遍历请求列表
	for _, requ := range reqs {
	    // 依据每个请求类型，校验权限
		switch tv := requ.Request.(type) {
		case *pb.RequestOp_RequestRange:
			if tv.RequestRange == nil {
				continue
			}

			if err := as.IsRangePermitted(ai, tv.RequestRange.Key, tv.RequestRange.RangeEnd); err != nil {
				return err
			}

		case *pb.RequestOp_RequestPut:
			if tv.RequestPut == nil {
				continue
			}

			if err := as.IsPutPermitted(ai, tv.RequestPut.Key); err != nil {
				return err
			}

		case *pb.RequestOp_RequestDeleteRange:
			if tv.RequestDeleteRange == nil {
				continue
			}

			if tv.RequestDeleteRange.PrevKv {
				err := as.IsRangePermitted(ai, tv.RequestDeleteRange.Key, tv.RequestDeleteRange.RangeEnd)
				if err != nil {
					return err
				}
			}

			err := as.IsDeleteRangePermitted(ai, tv.RequestDeleteRange.Key, tv.RequestDeleteRange.RangeEnd)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func checkTxnAuth(as auth.AuthStore, ai *auth.AuthInfo, rt *pb.TxnRequest) error {
    // 校验对key 的权限
	for _, c := range rt.Compare {
		if err := as.IsRangePermitted(ai, c.Key, c.RangeEnd); err != nil {
			return err
		}
	}

    // 校验成功的权限
	if err := checkTxnReqsPermission(as, ai, rt.Success); err != nil {
		return err
	}

    // 校验失败的权限
	return checkTxnReqsPermission(as, ai, rt.Failure)
}

func (aa *authApplierV3) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
    // 校验事务权限
	if err := checkTxnAuth(aa.as, &aa.authInfo, rt); err != nil {
		return nil, err
	}

    // 运行事务
	return aa.applierV3.Txn(rt)
}

// 移除租约
func (aa *authApplierV3) LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	if err := aa.checkLeasePuts(lease.LeaseID(lc.ID)); err != nil {
		return nil, err
	}
	return aa.applierV3.LeaseRevoke(lc)
}

// 校验租约权限
func (aa *authApplierV3) checkLeasePuts(leaseID lease.LeaseID) error {
	lease := aa.lessor.Lookup(leaseID)
	if lease != nil {
		for _, key := range lease.Keys() {
			if err := aa.as.IsPutPermitted(&aa.authInfo, []byte(key)); err != nil {
				return err
			}
		}
	}

	return nil
}

// 查询用户
func (aa *authApplierV3) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	err := aa.as.IsAdminPermitted(&aa.authInfo)
	if err != nil && r.Name != aa.authInfo.Username {
		aa.authInfo.Username = ""
		aa.authInfo.Revision = 0
		return &pb.AuthUserGetResponse{}, err
	}

	return aa.applierV3.UserGet(r)
}

// 角色查询
func (aa *authApplierV3) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	err := aa.as.IsAdminPermitted(&aa.authInfo)
	if err != nil && !aa.as.HasRole(aa.authInfo.Username, r.Role) {
		aa.authInfo.Username = ""
		aa.authInfo.Revision = 0
		return &pb.AuthRoleGetResponse{}, err
	}

	return aa.applierV3.RoleGet(r)
}

func needAdminPermission(r *pb.InternalRaftRequest) bool {
	switch {
	// 权限开启和关闭
	case r.AuthEnable != nil:
		return true
	case r.AuthDisable != nil:
		return true
	// 用户增删改
	case r.AuthUserAdd != nil:
		return true
	case r.AuthUserDelete != nil:
		return true
	case r.AuthUserChangePassword != nil:
		return true
	// 用户获取与释放角色
	case r.AuthUserGrantRole != nil:
		return true
	case r.AuthUserRevokeRole != nil:
		return true
	// 增加用户角色
	case r.AuthRoleAdd != nil:
		return true
	// 用户角色获取和释放和删除
	case r.AuthRoleGrantPermission != nil:
		return true
	case r.AuthRoleRevokePermission != nil:
		return true
	case r.AuthRoleDelete != nil:
		return true
	// 用户和用户角色列举
	case r.AuthUserList != nil:
		return true
	case r.AuthRoleList != nil:
		return true
	default:
		return false
	}
}

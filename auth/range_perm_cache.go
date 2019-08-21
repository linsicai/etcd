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

package auth

import (
	"go.etcd.io/etcd/auth/authpb"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/pkg/adt"

	"go.uber.org/zap"
)

func getMergedPerms(lg *zap.Logger, tx backend.BatchTx, userName string) *unifiedRangePermissions {
	// 查找用户
	user := getUser(lg, tx, userName)
	if user == nil {
		// 找不到
		return nil
	}

	// 初始化
	readPerms := &adt.IntervalTree{}
	writePerms := &adt.IntervalTree{}

	// 遍历用户角色
	for _, roleName := range user.Roles {
		role := getRole(tx, roleName)
		if role == nil {
			// 没有这个角色
			continue
		}

		// 遍历角色权限
		for _, perm := range role.KeyPermission {
			var ivl adt.Interval
			var rangeEnd []byte

			// 范围权限
			if len(perm.RangeEnd) != 1 || perm.RangeEnd[0] != 0 {
				rangeEnd = perm.RangeEnd
			}

			// 取adt
			if len(perm.RangeEnd) != 0 {
				ivl = adt.NewBytesAffineInterval(perm.Key, rangeEnd)
			} else {
				ivl = adt.NewBytesAffinePoint(perm.Key)
			}

			// 插入到权限列表中
			switch perm.PermType {
			case authpb.READWRITE:
				readPerms.Insert(ivl, struct{}{})
				writePerms.Insert(ivl, struct{}{})

			case authpb.READ:
				readPerms.Insert(ivl, struct{}{})

			case authpb.WRITE:
				writePerms.Insert(ivl, struct{}{})
			}
		}
	}

	return &unifiedRangePermissions{
		readPerms:  readPerms,
		writePerms: writePerms,
	}
}

// 校验权限
func checkKeyInterval(
	lg *zap.Logger,
	cachedPerms *unifiedRangePermissions,
	key, rangeEnd []byte,
	permtyp authpb.Permission_Type) bool {
	// 校验范围查找
	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
		rangeEnd = nil
	}

	// 找个列表查找
	ivl := adt.NewBytesAffineInterval(key, rangeEnd)
	switch permtyp {
	case authpb.READ:
		return cachedPerms.readPerms.Contains(ivl)
	case authpb.WRITE:
		return cachedPerms.writePerms.Contains(ivl)
	default:
		if lg != nil {
			lg.Panic("unknown auth type", zap.String("auth-type", permtyp.String()))
		} else {
			plog.Panicf("unknown auth type: %v", permtyp)
		}
	}

	return false
}

// 单点权限校验
func checkKeyPoint(lg *zap.Logger, cachedPerms *unifiedRangePermissions, key []byte, permtyp authpb.Permission_Type) bool {
	pt := adt.NewBytesAffinePoint(key)
	switch permtyp {
	case authpb.READ:
		return cachedPerms.readPerms.Intersects(pt)
	case authpb.WRITE:
		return cachedPerms.writePerms.Intersects(pt)
	default:
		if lg != nil {
			lg.Panic("unknown auth type", zap.String("auth-type", permtyp.String()))
		} else {
			plog.Panicf("unknown auth type: %v", permtyp)
		}
	}

	return false
}

func (as *authStore) isRangeOpPermitted(tx backend.BatchTx, userName string, key, rangeEnd []byte, permtyp authpb.Permission_Type) bool {
	// assumption: tx is Lock()ed
	_, ok := as.rangePermCache[userName]
	if !ok {
		// 从后端加载
		perms := getMergedPerms(as.lg, tx, userName)
		if perms == nil {
			if as.lg != nil {
				as.lg.Warn(
					"failed to create a merged permission",
					zap.String("user-name", userName),
				)
			} else {
				plog.Errorf("failed to create a unified permission of user %s", userName)
			}
			return false
		}
		as.rangePermCache[userName] = perms
	}

	if len(rangeEnd) == 0 {
		// 单点校验
		return checkKeyPoint(as.lg, as.rangePermCache[userName], key, permtyp)
	}

	// 范围校验
	return checkKeyInterval(as.lg, as.rangePermCache[userName], key, rangeEnd, permtyp)
}

// 清空
func (as *authStore) clearCachedPerm() {
	as.rangePermCache = make(map[string]*unifiedRangePermissions)
}

// 删除
func (as *authStore) invalidateCachedPerm(userName string) {
	delete(as.rangePermCache, userName)
}

// 统一范围权限
type unifiedRangePermissions struct {
	readPerms  *adt.IntervalTree
	writePerms *adt.IntervalTree
}

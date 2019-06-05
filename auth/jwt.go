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

package auth

import (
    "context"
    "crypto/ecdsa"
    "crypto/rsa"
    "errors"
    "time"

    jwt "github.com/dgrijalva/jwt-go"
    "go.uber.org/zap"
)

type tokenJWT struct {
    lg         *zap.Logger

    signMethod jwt.SigningMethod // 签名方法
    key        interface{} // key
    ttl        time.Duration // ttl
    verifyOnly bool // 仅验证
}

func (t *tokenJWT) enable()                         {}
func (t *tokenJWT) disable()                        {}
func (t *tokenJWT) invalidateUser(string)           {}
func (t *tokenJWT) genTokenPrefix() (string, error) { return "", nil }

func (t *tokenJWT) info(ctx context.Context, token string, rev uint64) (*AuthInfo, bool) {
    // rev isn't used in JWT, it is only used in simple token
    var (
        username string
        revision uint64
    )

    // 解析出key
    parsed, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
        if token.Method.Alg() != t.signMethod.Alg() {
            // 签名方法不匹配
            return nil, errors.New("invalid signing method")
        }

        // 返回key
        switch k := t.key.(type) {
        case *rsa.PrivateKey:
            return &k.PublicKey, nil
        case *ecdsa.PrivateKey:
            return &k.PublicKey, nil
        default:
            return t.key, nil
        }
    })

    // 解析失败
    if err != nil {
        if t.lg != nil {
            t.lg.Warn(
                "failed to parse a JWT token",
                zap.String("token", token),
                zap.Error(err),
            )
        } else {
            plog.Warningf("failed to parse jwt token: %s", err)
        }
        return nil, false
    }

    // 认证
    claims, ok := parsed.Claims.(jwt.MapClaims)
    if !parsed.Valid || !ok {
        if t.lg != nil {
            t.lg.Warn("invalid JWT token", zap.String("token", token))
        } else {
            plog.Warningf("invalid jwt token: %s", token)
        }
        return nil, false
    }

    // 成功
    username = claims["username"].(string)
    revision = uint64(claims["revision"].(float64))

    return &AuthInfo{Username: username, Revision: revision}, true
}

func (t *tokenJWT) assign(ctx context.Context, username string, revision uint64) (string, error) {
    if t.verifyOnly {
        // 仅验证
        return "", ErrVerifyOnly
    }

    // Future work: let a jwt token include permission information would be useful for
    // permission checking in proxy side.
    // 生成token
    tk := jwt.NewWithClaims(t.signMethod,
        jwt.MapClaims{
            "username": username,
            "revision": revision,
            "exp":      time.Now().Add(t.ttl).Unix(),
        })

    // 签名
    token, err := tk.SignedString(t.key)
    if err != nil {
        // 失败
        if t.lg != nil {
            t.lg.Warn(
                "failed to sign a JWT token",
                zap.String("user-name", username),
                zap.Uint64("revision", revision),
                zap.Error(err),
            )
        } else {
            plog.Debugf("failed to sign jwt token: %s", err)
        }
        return "", err
    }

    // 成功
    if t.lg != nil {
        t.lg.Info(
            "created/assigned a new JWT token",
            zap.String("user-name", username),
            zap.Uint64("revision", revision),
            zap.String("token", token),
        )
    } else {
        plog.Debugf("jwt token: %s", token)
    }

    return token, err
}

func newTokenProviderJWT(lg *zap.Logger, optMap map[string]string) (*tokenJWT, error) {
    var err error
    var opts jwtOptions

    // 解析配置
    err = opts.ParseWithDefaults(optMap)
    if err != nil {
        if lg != nil {
            lg.Warn("problem loading JWT options", zap.Error(err))
        } else {
            plog.Errorf("problem loading JWT options: %s", err)
        }
        return nil, ErrInvalidAuthOpts
    }

    // 找出不认识的key，告警
    var keys = make([]string, 0, len(optMap))
    for k := range optMap {
        if !knownOptions[k] {
            keys = append(keys, k)
        }
    }
    if len(keys) > 0 {
        if lg != nil {
            lg.Warn("unknown JWT options", zap.Strings("keys", keys))
        } else {
            plog.Warningf("unknown JWT options: %v", keys)
        }
    }

    // 生成key
    key, err := opts.Key()
    if err != nil {
        return nil, err
    }

    // 构建结构
    t := &tokenJWT{
        lg:         lg,
        ttl:        opts.TTL,
        signMethod: opts.SignMethod,
        key:        key,
    }

    // 公钥只能认证
    switch t.signMethod.(type) {
    case *jwt.SigningMethodECDSA:
        if _, ok := t.key.(*ecdsa.PublicKey); ok {
            t.verifyOnly = true
        }
    case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
        if _, ok := t.key.(*rsa.PublicKey); ok {
            t.verifyOnly = true
        }
    }

    return t, nil
}

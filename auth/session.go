package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// sessionTokenBytes 是会话 token 的熵（32 字节 = 256 bit）。
// 远超暴力猜测可行范围，无需再加签名或时间戳——token 本身就是凭据。
const sessionTokenBytes = 32

// NewSessionToken 生成一个新的会话 token 及其存储用哈希。
//
// 返回两个值是刻意的：**明文只发给客户端，库里只存哈希**。
//
// 为什么会话 token 也要哈希后存储（与密码同理，但常被忽略）：
// 会话 token 是等价于密码的凭据——拿到它就等于登录。若库里存明文，一次
// 只读的数据库泄漏（备份外泄、SQL 注入读表、运维误导出）就能让攻击者直接
// 冒充所有在线用户，而且**无声无息**：不需要破解、不触发任何登录失败告警。
// 存哈希后，泄漏的库对攻击者毫无用处（sha256 不可逆，且 token 是高熵随机数，
// 没有字典可查——这也是这里用 sha256 而非 argon2 的原因：抗字典攻击是密码
// 的需求，高熵随机 token 不需要，而每次请求都跑 argon2 会拖垮鉴权路径）。
func NewSessionToken() (token string, hash []byte, err error) {
	buf := make([]byte, sessionTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, fmt.Errorf("auth: 生成会话 token 失败: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(buf)
	return token, HashSessionToken(token), nil
}

// HashSessionToken 计算 token 的存储哈希。校验时对客户端传来的 token 做同样
// 变换后按哈希查表——**库里永远不出现明文 token**。
func HashSessionToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

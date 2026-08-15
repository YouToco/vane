// Package mailer sends security-sensitive account mail over authenticated TLS.
// It deliberately does not know about users, workspaces, or token persistence.
package mailer

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/mail"
	"net/smtp"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 15 * time.Second

// TLSMode selects one of the two encrypted SMTP transports. Plain SMTP is not
// represented and therefore cannot be selected accidentally.
type TLSMode string

const (
	TLSModeImplicit TLSMode = "implicit_tls"
	TLSModeSTARTTLS TLSMode = "starttls"
)

type Config struct {
	Host       string
	Port       int
	Username   string
	Password   string
	From       string
	TLSMode    TLSMode
	ServerName string
	Timeout    time.Duration
}

type Message struct {
	To      string
	Subject string
	Text    string
}

// Sender is safe for concurrent use. Credentials remain private fields and
// are never included in returned errors.
type Sender struct {
	cfg Config
}

func NewSMTP(cfg Config) (*Sender, error) {
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.ServerName = strings.TrimSpace(cfg.ServerName)
	if cfg.ServerName == "" {
		cfg.ServerName = cfg.Host
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 ||
		cfg.ServerName == "" || cfg.Username == "" || cfg.Password == "" {
		return nil, errors.New("mailer: SMTP 配置不完整")
	}
	if cfg.TLSMode != TLSModeImplicit && cfg.TLSMode != TLSModeSTARTTLS {
		return nil, errors.New("mailer: 只允许 implicit TLS 或 STARTTLS")
	}
	if _, err := mailbox(cfg.From); err != nil {
		return nil, fmt.Errorf("mailer: 发件地址无效: %w", err)
	}
	return &Sender{cfg: cfg}, nil
}

func (s *Sender) Send(ctx context.Context, message Message) error {
	if s == nil {
		return errors.New("mailer: sender 未配置")
	}
	to, err := mailbox(message.To)
	if err != nil {
		return fmt.Errorf("mailer: 收件地址无效: %w", err)
	}
	if err := safeHeader(message.Subject); err != nil {
		return fmt.Errorf("mailer: 邮件主题无效: %w", err)
	}
	if strings.TrimSpace(message.Subject) == "" || strings.TrimSpace(message.Text) == "" {
		return errors.New("mailer: 邮件主题和正文不能为空")
	}
	from, _ := mailbox(s.cfg.From)

	deadline := time.Now().Add(s.cfg.Timeout)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	conn, err := s.dial(ctx)
	if err != nil {
		return fmt.Errorf("mailer: 建立加密 SMTP 连接失败: %w", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("mailer: 设置 SMTP 截止时间失败: %w", err)
	}

	client, err := smtp.NewClient(conn, s.cfg.ServerName)
	if err != nil {
		return fmt.Errorf("mailer: 初始化 SMTP 会话失败: %w", err)
	}
	defer client.Close()
	if s.cfg.TLSMode == TLSModeSTARTTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("mailer: SMTP 服务未声明 STARTTLS")
		}
		if err := client.StartTLS(s.tlsConfig()); err != nil {
			return fmt.Errorf("mailer: STARTTLS 升级失败: %w", err)
		}
	}
	if ok, _ := client.Extension("AUTH"); !ok {
		return errors.New("mailer: SMTP 服务未声明 AUTH")
	}
	if err := client.Auth(smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.ServerName)); err != nil {
		return fmt.Errorf("mailer: SMTP 认证失败: %w", err)
	}
	if err := client.Mail(from); err != nil {
		return fmt.Errorf("mailer: SMTP 发件人被拒绝: %w", err)
	}
	if err := client.Rcpt(to); err != nil {
		return fmt.Errorf("mailer: SMTP 收件人被拒绝: %w", err)
	}
	body, err := client.Data()
	if err != nil {
		return fmt.Errorf("mailer: SMTP 正文通道失败: %w", err)
	}
	if _, err := io.WriteString(body, renderMessage(from, to, message.Subject, message.Text)); err != nil {
		_ = body.Close()
		return fmt.Errorf("mailer: 写入邮件正文失败: %w", err)
	}
	if err := body.Close(); err != nil {
		return fmt.Errorf("mailer: 提交邮件正文失败: %w", err)
	}
	if err := client.Quit(); err != nil {
		return fmt.Errorf("mailer: 结束 SMTP 会话失败: %w", err)
	}
	return nil
}

func (s *Sender) dial(ctx context.Context) (net.Conn, error) {
	address := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	dialer := &net.Dialer{Timeout: s.cfg.Timeout}
	if s.cfg.TLSMode == TLSModeImplicit {
		return (&tls.Dialer{NetDialer: dialer, Config: s.tlsConfig()}).DialContext(ctx, "tcp", address)
	}
	return dialer.DialContext(ctx, "tcp", address)
}

func (s *Sender) tlsConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: s.cfg.ServerName}
}

func mailbox(raw string) (string, error) {
	if err := safeHeader(raw); err != nil {
		return "", err
	}
	address, err := mail.ParseAddress(strings.TrimSpace(raw))
	if err != nil || address.Address == "" {
		return "", errors.New("地址格式无效")
	}
	if strings.ContainsAny(address.Address, "\r\n") {
		return "", errors.New("地址含换行")
	}
	return address.Address, nil
}

func safeHeader(value string) error {
	if strings.ContainsAny(value, "\r\n") {
		return errors.New("header 含换行")
	}
	return nil
}

func renderMessage(from, to, subject, text string) string {
	var random [16]byte
	_, _ = rand.Read(random[:])
	messageID := hex.EncodeToString(random[:]) + "@vane.local"
	var out strings.Builder
	w := bufio.NewWriter(&out)
	fmt.Fprintf(w, "From: %s\r\n", from)
	fmt.Fprintf(w, "To: %s\r\n", to)
	fmt.Fprintf(w, "Subject: %s\r\n", subject)
	fmt.Fprintf(w, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(w, "Message-ID: <%s>\r\n", messageID)
	fmt.Fprint(w, "MIME-Version: 1.0\r\n")
	fmt.Fprint(w, "Content-Type: text/plain; charset=UTF-8\r\n")
	fmt.Fprint(w, "Content-Transfer-Encoding: 8bit\r\n\r\n")
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	fmt.Fprint(w, strings.ReplaceAll(text, "\n", "\r\n"))
	fmt.Fprint(w, "\r\n")
	_ = w.Flush()
	return out.String()
}

package kafka

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func TestSecurityValidation(t *testing.T) {
	t.Parallel()
	valid := SecurityConfig{TLS: TLSConfig{Enabled: true}, SASL: SASLConfig{
		Mechanism: SASLPlain, Username: "user", Password: "password",
	}}
	for _, mechanism := range []SASLMechanism{SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512, " scram-sha-512 "} {
		config := valid
		config.SASL.Mechanism = mechanism
		if err := config.Validate(); err != nil {
			t.Fatalf("supported mechanism: %v", err)
		}
	}
	if err := (SecurityConfig{}).Validate(); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		edit func(*SecurityConfig)
	}{
		{"SASL needs TLS", func(c *SecurityConfig) { c.TLS.Enabled = false }},
		{"unknown mechanism", func(c *SecurityConfig) { c.SASL.Mechanism = "not-supported" }},
		{"unused credentials", func(c *SecurityConfig) { c.SASL.Mechanism = "" }},
		{"missing username", func(c *SecurityConfig) { c.SASL.Username = "" }},
		{"blank username", func(c *SecurityConfig) { c.SASL.Username = " " }},
		{"NUL username", func(c *SecurityConfig) { c.SASL.Username = "u\x00ser" }},
		{"missing password", func(c *SecurityConfig) { c.SASL.Password = "" }},
		{"NUL password", func(c *SecurityConfig) { c.SASL.Password = "pass\x00word" }},
		{"invalid UTF8 password", func(c *SecurityConfig) { c.SASL.Password = string([]byte{0xff}) }},
		{"oversized password", func(c *SecurityConfig) { c.SASL.Password = strings.Repeat("x", maxCredentialBytes+1) }},
		{"conflicting sources", func(c *SecurityConfig) { c.SASL.PasswordFile = "/secret" }},
		{"certificate without key", func(c *SecurityConfig) { c.TLS.CertFile = "/cert" }},
		{"key without certificate", func(c *SecurityConfig) { c.TLS.KeyFile = "/key" }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			config := valid
			check.edit(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	for _, config := range []TLSConfig{{CAFile: "ca"}, {CertFile: "cert", KeyFile: "key"}, {ServerName: "name"}} {
		if err := (SecurityConfig{TLS: config}).Validate(); err == nil {
			t.Fatal("TLS settings were silently ignored with TLS disabled")
		}
	}
}

func TestSecurityConstructionRejectsBadFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bad := filepath.Join(dir, "secret-content")
	if err := os.WriteFile(bad, []byte("sensitive-invalid-pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(dir, "missing"), dir, bad} {
		_, err := newClientSecurity(SecurityConfig{TLS: TLSConfig{Enabled: true, CAFile: path}})
		if err == nil || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "sensitive-invalid-pem") {
			t.Fatalf("expected safe CA-file error, got %v", err)
		}
	}
	large := filepath.Join(dir, "large")
	file, err := os.Create(large)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxTLSFileBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := newClientSecurity(SecurityConfig{TLS: TLSConfig{Enabled: true, CAFile: large}}); err == nil {
		t.Fatal("oversized CA file accepted")
	}
	if _, err := newClientSecurity(SecurityConfig{TLS: TLSConfig{Enabled: true, CertFile: bad, KeyFile: bad}}); err == nil {
		t.Fatal("invalid key pair accepted")
	}
}

func TestSecurityPasswordFileAndMechanismSnapshot(t *testing.T) {
	t.Parallel()
	for _, ending := range []string{"", "\n", "\r\n"} {
		t.Run(fmt.Sprintf("ending-%q", ending), func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "password")
			if err := os.WriteFile(file, []byte(" pass word "+ending), 0o600); err != nil {
				t.Fatal(err)
			}
			security, err := newClientSecurity(SecurityConfig{TLS: TLSConfig{Enabled: true}, SASL: SASLConfig{
				Mechanism: SASLPlain, Username: "user", PasswordFile: file,
			}})
			if err != nil {
				t.Fatal(err)
			}
			// Rotation after construction must not change an existing snapshot.
			if err := os.WriteFile(file, []byte("replacement"), 0o600); err != nil {
				t.Fatal(err)
			}
			client, err := kgo.NewClient(security.options...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
			_, first, err := mechanisms[0].Authenticate(context.Background(), "unused")
			if err != nil || string(first) != "\x00user\x00 pass word " {
				t.Fatal("PLAIN snapshot changed or password whitespace was lost")
			}
		})
	}
	for _, password := range []string{"", "\n", "\r\n", "has\x00nul", strings.Repeat("x", maxCredentialBytes+1)} {
		file := filepath.Join(t.TempDir(), "password")
		if err := os.WriteFile(file, []byte(password), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := newClientSecurity(SecurityConfig{TLS: TLSConfig{Enabled: true}, SASL: SASLConfig{
			Mechanism: SASLPlain, Username: "user", PasswordFile: file,
		}})
		if err == nil {
			t.Fatal("invalid password file accepted")
		}
	}
	for _, name := range []SASLMechanism{SASLSCRAMSHA256, SASLSCRAMSHA512} {
		security, err := newClientSecurity(SecurityConfig{TLS: TLSConfig{Enabled: true}, SASL: SASLConfig{
			Mechanism: name, Username: "user", Password: "password",
		}})
		if err != nil {
			t.Fatal(err)
		}
		client, err := kgo.NewClient(security.options...)
		if err != nil {
			t.Fatal(err)
		}
		mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
		if len(mechanisms) != 1 || mechanisms[0].Name() != string(name) {
			t.Fatal("wrong SCRAM mechanism or unexpected fallback")
		}
		client.Close()
	}
}

func TestSecurityRedactionPreservesErrorIdentity(t *testing.T) {
	t.Parallel()
	config := SASLConfig{Mechanism: SASLPlain, Username: "sensitive-user", Password: "sensitive-password", PasswordFile: "/sensitive-path"}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("config", "sasl", config)
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{fmt.Sprint(config), fmt.Sprintf("%+v", config), fmt.Sprintf("%#v", config), string(encoded), log.String(), fmt.Sprintf("%+v", SecurityConfig{SASL: config})} {
		for _, secret := range []string{config.Username, config.Password, config.PasswordFile} {
			if strings.Contains(output, secret) {
				t.Fatal("SASL config formatting disclosed credentials")
			}
		}
	}
	security := clientSecurity{redact: credentialRedactor(config.Password, config.Username)}
	original := fmt.Errorf("%w: %s %s", context.DeadlineExceeded, config.Username, config.Password)
	safe := security.safeError(original)
	if !errors.Is(safe, context.DeadlineExceeded) {
		t.Fatal("redaction lost cancellation identity")
	}
	if strings.Contains(safe.Error(), config.Username) || strings.Contains(fmt.Sprintf("%#v", safe), config.Password) {
		t.Fatal("adapter error disclosed credentials")
	}
	auth := fmt.Errorf("%w: server echoed %s", kerr.SaslAuthenticationFailed, config.Password)
	if got := security.safeError(auth); got != kerr.SaslAuthenticationFailed {
		t.Fatal("SASL error details were not suppressed")
	}
	if security.safeError(nil) != nil {
		t.Fatal("nil error changed")
	}
}

type securityCertificates struct {
	ca, cert, key string
	server        tls.Certificate
	roots         *x509.CertPool
}

func makeSecurityCertificates(t *testing.T, expired bool) securityCertificates {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "test-client"},
		DNSNames: []string{"kafka.test", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	if expired {
		leaf.NotAfter = now.Add(-time.Minute)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	files := securityCertificates{ca: filepath.Join(dir, "ca.pem"), cert: filepath.Join(dir, "cert.pem"), key: filepath.Join(dir, "key.pem"), roots: x509.NewCertPool()}
	for path, data := range map[string][]byte{files.ca: caPEM, files.cert: certPEM, files.key: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	files.server, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	files.roots.AppendCertsFromPEM(caPEM)
	return files
}

func handshakeSecurity(clientConfig, serverConfig *tls.Config) error {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(3 * time.Second))
	_ = serverConn.SetDeadline(time.Now().Add(3 * time.Second))
	result := make(chan error, 1)
	go func() { result <- tls.Server(serverConn, serverConfig).Handshake() }()
	clientErr := tls.Client(clientConn, clientConfig).Handshake()
	if clientErr != nil {
		_ = clientConn.Close()
	}
	return errors.Join(clientErr, <-result)
}

func TestTLSVerificationAndMutualTLS(t *testing.T) {
	t.Parallel()
	files := makeSecurityCertificates(t, false)
	server := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{files.server}, SessionTicketsDisabled: true}
	for _, check := range []struct {
		name    string
		config  TLSConfig
		mutual  bool
		wantErr bool
	}{
		{"trusted", TLSConfig{Enabled: true, CAFile: files.ca, ServerName: "kafka.test"}, false, false},
		{"wrong name", TLSConfig{Enabled: true, CAFile: files.ca, ServerName: "other.test"}, false, true},
		{"untrusted", TLSConfig{Enabled: true, ServerName: "kafka.test"}, false, true},
		{"mutual", TLSConfig{Enabled: true, CAFile: files.ca, CertFile: files.cert, KeyFile: files.key, ServerName: "kafka.test"}, true, false},
		{"missing client certificate", TLSConfig{Enabled: true, CAFile: files.ca, ServerName: "kafka.test"}, true, true},
	} {
		t.Run(check.name, func(t *testing.T) {
			client, err := check.config.load()
			if err != nil {
				t.Fatal(err)
			}
			if client.InsecureSkipVerify || client.MinVersion < tls.VersionTLS12 {
				t.Fatal("unsafe TLS defaults")
			}
			peer := server.Clone()
			if check.mutual {
				peer.ClientAuth, peer.ClientCAs = tls.RequireAndVerifyClientCert, files.roots
			}
			if err := handshakeSecurity(client, peer); (err != nil) != check.wantErr {
				t.Fatalf("handshake error = %v, want error %v", err, check.wantErr)
			}
		})
	}
	expired := makeSecurityCertificates(t, true)
	client, err := (TLSConfig{Enabled: true, CAFile: expired.ca, ServerName: "kafka.test"}).load()
	if err != nil {
		t.Fatal(err)
	}
	server.Certificates = []tls.Certificate{expired.server}
	if err := handshakeSecurity(client, server); err == nil {
		t.Fatal("expired certificate was accepted")
	}
}

type securityListener struct{}

func (securityListener) Assigned(map[string][]int32) {}
func (securityListener) Revoked(map[string][]int32)  {}
func (securityListener) Lost(map[string][]int32)     {}
func (securityListener) RebalanceBlocked()           {}

func TestBothKafkaAdaptersUseSecuritySnapshot(t *testing.T) {
	t.Parallel()
	files := makeSecurityCertificates(t, false)
	security := SecurityConfig{TLS: TLSConfig{Enabled: true, CAFile: files.ca},
		SASL: SASLConfig{Mechanism: SASLSCRAMSHA512, Username: "user", Password: "password"}}
	pub, err := NewPublisher(Config{Brokers: []string{"127.0.0.1:1"}, PublishTimeout: time.Second, Security: security})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	factory, err := NewConsumerFactory(ConsumerConfig{Brokers: []string{"127.0.0.1:1"}, Topics: []string{"events"},
		Group: "test", SessionTimeout: 10 * time.Second, RebalanceTimeout: 30 * time.Second,
		FetchMaxWait: time.Second, Security: security})
	if err != nil {
		t.Fatal(err)
	}
	// All future workers must use the snapshot even if files disappear.
	if err := os.Remove(files.ca); err != nil {
		t.Fatal(err)
	}
	source, err := factory.NewSource("worker", securityListener{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for _, client := range []*kgo.Client{pub.client, source.(*consumerSource).client} {
		config := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
		if config == nil || config.RootCAs == nil || config.ServerName != "" {
			t.Fatal("TLS settings or automatic broker-name verification were lost")
		}
		mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
		if len(mechanisms) != 1 || mechanisms[0].Name() != string(SASLSCRAMSHA512) {
			t.Fatal("SASL settings were lost")
		}
	}
	if pub.client.OptValue(kgo.RecordDeliveryTimeout).(time.Duration) != time.Second {
		t.Fatal("security changed the publish timeout")
	}
	if pub.client.OptValue(kgo.RecordRetries).(int64) != 0 {
		t.Fatal("security enabled extra producer retries")
	}
}

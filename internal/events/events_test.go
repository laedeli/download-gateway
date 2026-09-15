package events

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// dial replaces the producer constructor for one test, records what it was
// given and hands back a mock producer instead of dialing brokers.
type dial struct {
	called bool
	addrs  []string
	cfg    *sarama.Config
	mock   *mocks.SyncProducer
}

func fakeDial(t *testing.T) *dial {
	t.Helper()
	d := &dial{}
	prev := newSyncProducer
	newSyncProducer = func(addrs []string, cfg *sarama.Config) (sarama.SyncProducer, error) {
		d.called, d.addrs, d.cfg = true, addrs, cfg
		d.mock = mocks.NewSyncProducer(t, cfg)
		return d.mock, nil
	}
	t.Cleanup(func() { newSyncProducer = prev })
	return d
}

func TestNewPublisherWithoutBrokersLogsOnly(t *testing.T) {
	d := fakeDial(t)
	pub, err := NewPublisher(Config{TopicPrefix: "tenant."})
	if err != nil {
		t.Fatal(err)
	}
	if d.called || pub.p != nil {
		t.Fatal("built a producer without brokers")
	}
	if !pub.Ready() {
		t.Error("log-only mode must count as ready")
	}
	if err := pub.EmitStarted(context.Background(), Started{ClientID: "1", Adapter: "usenet"}); err != nil {
		t.Errorf("log-only emit: %v", err)
	}
}

// Brokers without any TLS file are a plaintext listener: the gateway publishes
// to it rather than only logging.
func TestNewPublisherWithoutTLSFilesPublishesInPlaintext(t *testing.T) {
	d := fakeDial(t)
	pub, err := NewPublisher(Config{Brokers: "kafka-a:9092,kafka-b:9092", TopicPrefix: "tenant."})
	if err != nil {
		t.Fatal(err)
	}
	if !d.called || pub.p == nil {
		t.Fatal("brokers are set but no producer was built")
	}
	if !slices.Equal(d.addrs, []string{"kafka-a:9092", "kafka-b:9092"}) {
		t.Errorf("dialed %v", d.addrs)
	}
	if d.cfg.Net.TLS.Enable || d.cfg.Net.TLS.Config != nil {
		t.Error("plaintext brokers got TLS")
	}
	if !d.cfg.Producer.Idempotent || d.cfg.Producer.RequiredAcks != sarama.WaitForAll {
		t.Error("the plaintext producer lost its delivery settings")
	}

	d.mock.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(m *sarama.ProducerMessage) error {
		if m.Topic != "tenant.download.client.started" {
			return fmt.Errorf("topic %s", m.Topic)
		}
		return nil
	})
	if err := pub.EmitStarted(context.Background(), Started{ClientID: "1", Adapter: "usenet", Title: "Example"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Close(); err != nil { // the mock reports unmet expectations here
		t.Fatal(err)
	}
}

func TestNewPublisherWithTLSFilesUsesMTLS(t *testing.T) {
	cert, key := writeClientCert(t)
	d := fakeDial(t)
	pub, err := NewPublisher(Config{Brokers: "kafka:9093", CertFile: cert, KeyFile: key, CAFile: cert})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if !d.called {
		t.Fatal("no producer was built")
	}
	tlsCfg := d.cfg.Net.TLS.Config
	if !d.cfg.Net.TLS.Enable || tlsCfg == nil || len(tlsCfg.Certificates) != 1 || tlsCfg.RootCAs == nil {
		t.Errorf("want mTLS with the client certificate and the CA, got enable=%v config=%v", d.cfg.Net.TLS.Enable, tlsCfg)
	}
}

// TLS files that do not add up to a client certificate fail at start instead of
// quietly connecting in plaintext.
func TestNewPublisherRefusesIncompleteTLS(t *testing.T) {
	cert, _ := writeClientCert(t)
	for name, c := range map[string]Config{
		"ca only":      {Brokers: "kafka:9093", CAFile: cert},
		"no key":       {Brokers: "kafka:9093", CertFile: cert},
		"missing file": {Brokers: "kafka:9093", CertFile: cert, KeyFile: filepath.Join(t.TempDir(), "user.key")},
	} {
		t.Run(name, func(t *testing.T) {
			d := fakeDial(t)
			if _, err := NewPublisher(c); err == nil {
				t.Error("want an error")
			}
			if d.called {
				t.Error("built a producer anyway")
			}
		})
	}
}

// writeClientCert writes a self-signed certificate and its key as PEM files.
func writeClientCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "download-gateway"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "user.crt"), filepath.Join(dir, "user.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

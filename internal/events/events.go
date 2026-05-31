// Package events publishes the download lifecycle on Kafka topics
// stube.download.client.{started,progress,completed,failed} per
// ADR-009/ADR-019. Authentication to the platform-kafka cluster is
// mTLS (Strimzi-issued client cert from the stube-download-gateway
// KafkaUser secret) — NOT OAuth/SASL.
//
// v1 wire format is JSON keyed by "adapter:client_id" (the composite
// key per ADR-020 F3). The Avro schemas in stube/schemas/stube/download/
// are the contract; this switches to Avro-with-Apicurio once the
// schemas repo's publish-to-apicurio step is wired (it's a placeholder
// today). Consumers (katalog-ingest, katalog-manager) don't exist yet,
// so JSON-now / Avro-later is contained.
package events

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const topicPrefix = "stube.download.client."

// Event kinds map to the four topics.
const (
	KindStarted   = "started"
	KindProgress  = "progress"
	KindCompleted = "completed"
	KindFailed    = "failed"
)

// Started/Progress/Completed/Failed payloads mirror the Avro records in
// stube/schemas/stube/download/*.avsc (snake_case, ms timestamps).
type Started struct {
	ClientID     string `json:"client_id"`
	Adapter      string `json:"adapter"`
	WantedItemID string `json:"wanted_item_id,omitempty"`
	Title        string `json:"title"`
	SizeBytes    *int64 `json:"size_bytes"`
	StartedAt    int64  `json:"started_at"`
}

type Progress struct {
	ClientID        string  `json:"client_id"`
	Adapter         string  `json:"adapter"`
	State           string  `json:"state"`
	ProgressPct     float64 `json:"progress_pct"`
	DownloadedBytes int64   `json:"downloaded_bytes"`
	SizeBytes       *int64  `json:"size_bytes"`
	SpeedBps        *int64  `json:"speed_bps"`
	EtaSec          *int32  `json:"eta_sec"`
	EmittedAt       int64   `json:"emitted_at"`
}

type Completed struct {
	ClientID     string   `json:"client_id"`
	Adapter      string   `json:"adapter"`
	WantedItemID string   `json:"wanted_item_id,omitempty"`
	Files        []string `json:"files"`
	SizeBytes    int64    `json:"size_bytes"`
	CompletedAt  int64    `json:"completed_at"`
}

type Failed struct {
	ClientID  string `json:"client_id"`
	Adapter   string `json:"adapter"`
	Error     string `json:"error"`
	ErrorCode string `json:"error_code,omitempty"`
	Retriable bool   `json:"retriable"`
	FailedAt  int64  `json:"failed_at"`
}

// Publisher wraps sarama.SyncProducer. A nil internal producer means
// scaffold mode (logs instead of publishing) so the gateway can run
// locally without Kafka.
type Publisher struct {
	p sarama.SyncProducer
}

// Config controls how the producer authenticates.
type Config struct {
	Brokers string // comma-separated host:port (TLS listener :9093)
	// mTLS material. Mounted from the KafkaUser secret. When CertFile is
	// empty the publisher runs in scaffold (log-only) mode.
	CertFile string
	KeyFile  string
	CAFile   string
}

// ConfigFromEnv reads the standard env wiring (set by the Deployment).
func ConfigFromEnv() Config {
	return Config{
		Brokers:  os.Getenv("KAFKA_BROKERS"),
		CertFile: os.Getenv("KAFKA_TLS_CERT"),
		KeyFile:  os.Getenv("KAFKA_TLS_KEY"),
		CAFile:   os.Getenv("KAFKA_TLS_CA"),
	}
}

// NewPublisher connects to Kafka over mTLS. If brokers or cert material
// is missing it returns a scaffold publisher (nil producer) rather than
// an error, so the service stays up and logs what it would emit.
func NewPublisher(c Config) (*Publisher, error) {
	if c.Brokers == "" || c.CertFile == "" {
		slog.Warn("kafka not configured; running events in scaffold (log-only) mode",
			"brokers_set", c.Brokers != "", "cert_set", c.CertFile != "")
		return &Publisher{}, nil
	}
	tlsCfg, err := mTLS(c)
	if err != nil {
		return nil, fmt.Errorf("kafka mTLS: %w", err)
	}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsCfg
	cfg.Version = sarama.V3_5_0_0

	p, err := sarama.NewSyncProducer(strings.Split(c.Brokers, ","), cfg)
	if err != nil {
		return nil, err
	}
	slog.Info("kafka producer connected (mTLS)", "brokers", c.Brokers)
	return &Publisher{p: p}, nil
}

func mTLS(c Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if c.CAFile != "" {
		ca, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("append CA failed")
		}
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

func nowMs() int64 { return time.Now().UnixMilli() }

// key is the composite (adapter, client_id) per ADR-020 F3.
func key(adapter, clientID string) []byte { return []byte(adapter + ":" + clientID) }

func (p *Publisher) emit(ctx context.Context, kind, adapter, clientID string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	topic := topicPrefix + kind
	if p == nil || p.p == nil {
		slog.Debug("publish (scaffold no-op)", "topic", topic, "key", adapter+":"+clientID, "bytes", len(b))
		return nil
	}
	_, _, err = p.p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key(adapter, clientID)),
		Value: sarama.ByteEncoder(b),
	})
	return err
}

func (p *Publisher) EmitStarted(ctx context.Context, e Started) error {
	e.StartedAt = nowMs()
	return p.emit(ctx, KindStarted, e.Adapter, e.ClientID, e)
}

func (p *Publisher) EmitProgress(ctx context.Context, e Progress) error {
	e.EmittedAt = nowMs()
	return p.emit(ctx, KindProgress, e.Adapter, e.ClientID, e)
}

func (p *Publisher) EmitCompleted(ctx context.Context, e Completed) error {
	e.CompletedAt = nowMs()
	return p.emit(ctx, KindCompleted, e.Adapter, e.ClientID, e)
}

func (p *Publisher) EmitFailed(ctx context.Context, e Failed) error {
	e.FailedAt = nowMs()
	return p.emit(ctx, KindFailed, e.Adapter, e.ClientID, e)
}

// Close releases the producer.
func (p *Publisher) Close() error {
	if p == nil || p.p == nil {
		return nil
	}
	return p.p.Close()
}

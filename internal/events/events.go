// Package events publishes the download lifecycle on Kafka topics
// stube.download.client.{started,progress,completed,failed} per
// ADR-009/ADR-019. Authentication to a shared cluster is mTLS (a client
// cert, e.g. from a Strimzi KafkaUser secret) — NOT OAuth/SASL; a
// plaintext listener (a bundled broker) needs no TLS material at all.
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
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// defaultTopicPrefix keeps prod (single-tenant stube) byte-identical. On a
// shared cluster the tenant sets KAFKA_TOPIC_PREFIX (e.g. "zaentrum-beta.") so
// events land under its own namespace; the full topic is <prefix>download.client.<kind>.
const defaultTopicPrefix = "stube."

// Event kinds map to the four topics.
const (
	KindStarted   = "started"
	KindProgress  = "progress"
	KindCompleted = "completed"
	KindFailed    = "failed"
)

// Started/Progress/Completed/Failed payloads mirror the Avro records in
// stube/schemas/stube/download/*.avsc (snake_case, ms timestamps).
//
// Adapter is the client id — the field name predates configurable clients, and
// an env client's id is still its type name. ClientType is the adapter type.
type Started struct {
	ClientID     string `json:"client_id"`
	Adapter      string `json:"adapter"`
	ClientType   string `json:"client_type"`
	WantedItemID string `json:"wanted_item_id,omitempty"`
	Title        string `json:"title"`
	SizeBytes    *int64 `json:"size_bytes"`
	StartedAt    int64  `json:"started_at"`
}

type Progress struct {
	ClientID     string `json:"client_id"`
	Adapter      string `json:"adapter"`
	ClientType   string `json:"client_type"`
	WantedItemID string `json:"wanted_item_id,omitempty"`
	Title        string `json:"title,omitempty"`
	State        string `json:"state"`
	// NativeState is the download client's own state string (qBittorrent
	// "stalledDL"/"pausedDL"/"metaDL", NZBGet "PAUSED"/"REPAIRING"/…). State is
	// the coarse fold; this is what a console shows a human.
	NativeState     string  `json:"native_state,omitempty"`
	ProgressPct     float64 `json:"progress_pct"`
	DownloadedBytes int64   `json:"downloaded_bytes"`
	SizeBytes       *int64  `json:"size_bytes"`
	// SpeedBps is always present: 0 means "idle", not "unknown". (Size and eta
	// keep pointer semantics because those genuinely can be unknown.)
	SpeedBps int64  `json:"speed_bps"`
	EtaSec   *int32 `json:"eta_sec"`
	// Peers/health, when the client reports them (torrents: seeds/peers/ratio;
	// usenet: article health 0-1000).
	Seeders   *int32 `json:"seeders,omitempty"`
	Leechers  *int32 `json:"leechers,omitempty"`
	Health    *int32 `json:"health,omitempty"`
	EmittedAt int64  `json:"emitted_at"`
}

type Completed struct {
	ClientID     string   `json:"client_id"`
	Adapter      string   `json:"adapter"`
	ClientType   string   `json:"client_type"`
	WantedItemID string   `json:"wanted_item_id,omitempty"`
	Files        []string `json:"files"`
	SizeBytes    int64    `json:"size_bytes"`
	CompletedAt  int64    `json:"completed_at"`
}

type Failed struct {
	ClientID     string `json:"client_id"`
	Adapter      string `json:"adapter"`
	ClientType   string `json:"client_type"`
	WantedItemID string `json:"wanted_item_id,omitempty"`
	Title        string `json:"title,omitempty"`
	Error        string `json:"error"`
	ErrorCode    string `json:"error_code,omitempty"`
	Retriable    bool   `json:"retriable"`
	FailedAt     int64  `json:"failed_at"`
}

// Publisher wraps sarama.SyncProducer. A nil internal producer means
// scaffold mode (logs instead of publishing) so the gateway can run
// locally without Kafka.
type Publisher struct {
	p      sarama.SyncProducer
	prefix string // <prefix>download.client.<kind>; defaults to "stube." when blank
	closed atomic.Bool
}

// Ready reports whether the publisher can take events: it was built and has
// not been closed. Scaffold (log-only) mode counts as ready — running without
// Kafka is a supported configuration, not an outage.
func (p *Publisher) Ready() bool {
	return p != nil && !p.closed.Load()
}

// topicFor builds the full topic name for an event kind. Nil-safe: emit's
// defensive p==nil path still resolves a topic string for the scaffold log.
func (p *Publisher) topicFor(kind string) string {
	prefix := defaultTopicPrefix
	if p != nil && p.prefix != "" {
		prefix = p.prefix
	}
	return prefix + "download.client." + kind
}

// Config controls how the producer authenticates.
type Config struct {
	// Brokers is a comma-separated host:port list. Empty runs the publisher in
	// scaffold (log-only) mode.
	Brokers string
	// mTLS material, e.g. the files of a KafkaUser secret. With none of the
	// three set the producer connects in plaintext; with any of them set it
	// uses mTLS and needs the certificate and key.
	CertFile string
	KeyFile  string
	CAFile   string
	// TopicPrefix is the per-tenant namespace (default "stube."). The four
	// topics are <TopicPrefix>download.client.{started,progress,completed,failed}.
	TopicPrefix string
}

// ConfigFromEnv reads the standard env wiring (set by the Deployment).
func ConfigFromEnv() Config {
	prefix := strings.TrimSpace(os.Getenv("KAFKA_TOPIC_PREFIX"))
	if prefix == "" {
		prefix = defaultTopicPrefix
	}
	return Config{
		Brokers:     os.Getenv("KAFKA_BROKERS"),
		CertFile:    os.Getenv("KAFKA_TLS_CERT"),
		KeyFile:     os.Getenv("KAFKA_TLS_KEY"),
		CAFile:      os.Getenv("KAFKA_TLS_CA"),
		TopicPrefix: prefix,
	}
}

// NewPublisher connects to Kafka: over mTLS when TLS material is configured,
// in plaintext when none is. Without brokers it returns a scaffold publisher
// (nil producer) rather than an error, so the service stays up and logs what
// it would emit.
func NewPublisher(c Config) (*Publisher, error) {
	if c.Brokers == "" {
		slog.Warn("kafka not configured (no KAFKA_BROKERS); running events in scaffold (log-only) mode")
		return &Publisher{prefix: c.TopicPrefix}, nil
	}
	cfg, err := producerConfig(c)
	if err != nil {
		return nil, err
	}
	p, err := newSyncProducer(strings.Split(c.Brokers, ","), cfg)
	if err != nil {
		return nil, err
	}
	mode := "plaintext"
	if cfg.Net.TLS.Enable {
		mode = "mTLS"
	}
	slog.Info("kafka producer connected", "mode", mode, "brokers", c.Brokers, "topic_prefix", c.TopicPrefix)
	return &Publisher{p: p, prefix: c.TopicPrefix}, nil
}

// newSyncProducer is sarama's constructor, which dials the brokers; tests
// replace it to inspect the configuration without one.
var newSyncProducer = sarama.NewSyncProducer

// producerConfig is the idempotent producer's configuration: mTLS when any TLS
// file is set, which then needs the certificate and key, so an incomplete set
// fails instead of falling back to plaintext.
func producerConfig(c Config) (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Version = sarama.V3_5_0_0
	if c.CertFile == "" && c.KeyFile == "" && c.CAFile == "" {
		return cfg, nil
	}
	tlsCfg, err := mTLS(c)
	if err != nil {
		return nil, fmt.Errorf("kafka mTLS: %w", err)
	}
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = tlsCfg
	return cfg, nil
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
	topic := p.topicFor(kind)
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
	if p == nil {
		return nil
	}
	p.closed.Store(true)
	if p.p == nil {
		return nil
	}
	return p.p.Close()
}

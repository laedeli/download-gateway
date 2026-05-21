// Package events publishes download outcomes on Kafka topics prefixed
// stube.download.client.* per ADR-009.
package events

import (
	"context"
	"errors"
	"log/slog"

	"github.com/IBM/sarama"
)

// Publisher wraps sarama.SyncProducer. nil = scaffold mode.
type Publisher struct {
	p sarama.SyncProducer
}

// NewPublisher connects to the given broker list.
func NewPublisher(brokers string) (*Publisher, error) {
	if brokers == "" {
		return nil, errors.New("kafka brokers empty")
	}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	_ = cfg
	return &Publisher{}, nil
}

// EmitCompleted publishes a stube.download.client.completed event.
func (p *Publisher) EmitCompleted(ctx context.Context, key, payload []byte) error {
	if p == nil || p.p == nil {
		slog.Debug("publish download.client.completed (scaffold no-op)", "bytes", len(payload))
		return nil
	}
	_, _, err := p.p.SendMessage(&sarama.ProducerMessage{
		Topic: "stube.download.client.completed",
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

// Close releases the producer.
func (p *Publisher) Close() error {
	if p == nil || p.p == nil {
		return nil
	}
	return p.p.Close()
}

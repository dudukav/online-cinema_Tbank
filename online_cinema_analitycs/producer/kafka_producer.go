package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

const Topic = "movie-events"

type KafkaProducer struct {
	writer *kafka.Writer
}

type KafkaEvent struct {
	EventID         string `json:"event_id"`
	UserID          string `json:"user_id"`
	MovieID         string `json:"movie_id"`
	EventType       string `json:"event_type"`
	TimestampMs     int64  `json:"timestamp_ms"`
	DeviceType      string `json:"device_type"`
	SessionID       string `json:"session_id"`
	ProgressSeconds int    `json:"progress_seconds"`
}

func NewKafkaProducer(brokers string) (*KafkaProducer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(splitBrokers(brokers)...),
		Topic:        Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &KafkaProducer{writer: writer}, nil
}

func (kp *KafkaProducer) ProduceEvent(ctx context.Context, event KafkaEvent) error {
	if err := validateKafkaEvent(event); err != nil {
		return err
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.UserID),
		Value: data,
		Time:  time.UnixMilli(event.TimestampMs).UTC(),
	}

	maxRetries := 3
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = kp.writer.WriteMessages(ctx, msg)
		if err == nil {
			log.Printf("published event_id=%s event_type=%s timestamp=%s",
				event.EventID,
				event.EventType,
				time.UnixMilli(event.TimestampMs).UTC().Format(time.RFC3339),
			)
			return nil
		}

		log.Printf("kafka publish failed attempt=%d event_id=%s error=%v", attempt+1, event.EventID, err)
		if attempt < maxRetries {
			jitter := time.Duration(50+rand.Intn(50)) * time.Millisecond
			time.Sleep(backoff + jitter)
			backoff *= 2
		}
	}

	return fmt.Errorf("publish event %s: %w", event.EventID, err)
}

func (kp *KafkaProducer) Close() error {
	return kp.writer.Close()
}

func validateKafkaEvent(event KafkaEvent) error {
	if event.EventID == "" || event.UserID == "" || event.EventType == "" || event.DeviceType == "" || event.TimestampMs <= 0 {
		return errInvalidEvent
	}
	if isViewEvent(event.EventType) && (event.MovieID == "" || event.SessionID == "") {
		return errInvalidEvent
	}
	return nil
}

func splitBrokers(brokers string) []string {
	if brokers == "" {
		return []string{"localhost:9092"}
	}

	result := make([]string, 0, 2)
	start := 0
	for i := 0; i <= len(brokers); i++ {
		if i == len(brokers) || brokers[i] == ',' {
			if start < i {
				result = append(result, brokers[start:i])
			}
			start = i + 1
		}
	}
	return result
}

var errInvalidEvent = fmt.Errorf("invalid event")

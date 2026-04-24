package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	pb "producer/proto/schema"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

var Topic = "movie-events"

type KafkaProducer struct {
	producer *kafka.Producer
}

func NewKafkaProducer(brokers string) (*KafkaProducer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"enable.idempotence": true,
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Delivered event_id=%s, event_type=%s, at=%v",
						ev.Key, ev.Value, time.Now())
				}
			}
		}
	}()

	return &KafkaProducer{producer: p}, nil
}

func (kp *KafkaProducer) ProduceEvent(ctx context.Context, event *pb.MovieEvent) error {
	if !isValidEvent(event) {
		return errInvalidEvent
	}

	data, err := proto.Marshal(event)
	if err != nil {
		return err
	}

	key := []byte(event.UserId)

	maxRetries := 3
	backoff := 100 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := kp.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &Topic,
				Partition: kafka.PartitionAny,
			},
			Key:   key,
			Value: data,
		}, nil)

		if err == nil {
			return nil
		}

		log.Printf("Kafka produce failed (attempt %d): %v", attempt+1, err)

		if attempt < maxRetries {
			jitter := time.Duration(50 + rand.Intn(50)) * time.Millisecond
			time.Sleep(backoff + jitter)
			backoff *= 2
		}
	}

	return err
}

var errInvalidEvent = fmt.Errorf("invalid event")

func isValidEvent(event *pb.MovieEvent) bool {
	if event.UserId == "" || event.EventId == "" {
		return false
	}
	if event.EventType == pb.EventType_VIEW_STARTED ||
		event.EventType == pb.EventType_VIEW_PAUSED ||
		event.EventType == pb.EventType_VIEW_RESUMED ||
		event.EventType == pb.EventType_VIEW_FINISHED {
		if event.MovieId == "" || event.SessionId == "" {
			return false
		}
	}
	return true
}
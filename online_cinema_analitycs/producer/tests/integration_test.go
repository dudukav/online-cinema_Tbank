package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

const (
	ProducerURL   = "http://localhost:8080/events"
	ClickHouseDSN = "tcp://localhost:9000"
)

type EventRequest struct {
	EventId        string      `json:"event_id"`
	UserId         string      `json:"user_id"`
	MovieId        string      `json:"movie_id"`
	EventType      string      `json:"event_type"`
	Timestamp      string      `json:"timestamp"`
	DeviceType     string      `json:"device_type"`
	SessionId      string      `json:"session_id"`
	ProgressSeconds int        `json:"progress_seconds"`
}

type ClickhouseEvent struct {
	EventId         string
	UserID          string
	MovieId         string
	EventType       string
	TimestampMs     int64
	DeviceType      string
	SessionID       string
	ProgressSeconds int32
}

func TestEndToEndEventFlow(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	if err != nil {
		t.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	truncate := "ALTER TABLE raw_events DELETE WHERE 1=1"
	if err := conn.Exec(context.Background(), truncate); err != nil {
		log.Printf("warn: truncate raw_events failed: %v", err)
	}

	eventId := "test_event_1"
	reqBody := EventRequest{
		EventId:         eventId,
		UserId:          "test_user_1",
		MovieId:         "test_movie_1",
		EventType:       "VIEW_STARTED",
		Timestamp:       "2026-04-24T12:00:00Z",
		DeviceType:      "MOBILE",
		SessionId:       "test_sess_1",
		ProgressSeconds: 0,
	}

	body, err := json.Marshal(reqBody)
	require.NoError(t, err)

	resp, err := http.Post(ProducerURL, "application/json", http.NoBody)
	resp, err = http.Post(
		ProducerURL,
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	time.Sleep(5 * time.Second)

	var result ClickhouseEvent
	err = conn.QueryRow(
		context.Background(),
		"SELECT event_id, user_id, movie_id, event_type, timestamp_ms, device_type, session_id, progress_seconds FROM raw_events WHERE event_id = $1",
		eventId,
	).Scan(
		&result.EventId,
		&result.UserID,
		&result.MovieId,
		&result.EventType,
		&result.TimestampMs,
		&result.DeviceType,
		&result.SessionID,
		&result.ProgressSeconds,
	)

	require.NoError(t, err)
	require.Equal(t, eventId, result.EventId)
	require.Equal(t, "VIEW_STARTED", result.EventType)
	require.NotZero(t, result.TimestampMs)
	require.Equal(t, "test_sess_1", result.SessionID)
	require.Equal(t, int32(0), result.ProgressSeconds)

	fmt.Printf("Integration test passed: event_id=%s\n", result.EventId)
}
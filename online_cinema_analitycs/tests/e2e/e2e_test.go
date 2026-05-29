package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

const (
	producerEventsURL = "http://127.0.0.1:8080/events"
	aggregatorURL     = "http://127.0.0.1:8082/aggregate"
	postgresDSN       = "postgresql://user:pass@127.0.0.1:15432/analytics?sslmode=disable"
)

type eventRequest struct {
	EventID         string `json:"event_id"`
	UserID          string `json:"user_id"`
	MovieID         string `json:"movie_id"`
	EventType       string `json:"event_type"`
	Timestamp       string `json:"timestamp"`
	DeviceType      string `json:"device_type"`
	SessionID       string `json:"session_id"`
	ProgressSeconds int    `json:"progress_seconds"`
}

type producerResponse struct {
	EventID      string `json:"event_id"`
	Status       string `json:"status"`
	TimestampUTC string `json:"timestamp_utc"`
}

type aggregateResponse struct {
	Status  string  `json:"status"`
	Date    string  `json:"date"`
	Records float64 `json:"records"`
	DAU     float64 `json:"dau"`
}

func TestMovieEventAggregationFlow(t *testing.T) {
	if os.Getenv("RUN_E2E_TESTS") != "1" {
		t.Skip("set RUN_E2E_TESTS=1 after docker compose up")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ch := openClickHouse(t)
	defer ch.Close()
	pg := openPostgres(t)
	defer pg.Close()

	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	eventTime := time.Date(2035, 1, 1, 12, 0, 0, 0, time.UTC)
	date := eventTime.Format("2006-01-02")
	userID := "e2e_user_" + suffix
	movieID := "e2e_movie_" + suffix
	sessionID := "e2e_session_" + suffix

	cleanupTestData(t, ctx, ch, pg, date, suffix)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupTestData(t, cleanupCtx, ch, pg, date, suffix)
	})

	startedEventID := "e2e_event_started_" + suffix
	finishedEventID := "e2e_event_finished_" + suffix

	postEvent(t, eventRequest{
		EventID:         startedEventID,
		UserID:          userID,
		MovieID:         movieID,
		EventType:       "VIEW_STARTED",
		Timestamp:       eventTime.Format(time.RFC3339),
		DeviceType:      "MOBILE",
		SessionID:       sessionID,
		ProgressSeconds: 0,
	})
	postEvent(t, eventRequest{
		EventID:         finishedEventID,
		UserID:          userID,
		MovieID:         movieID,
		EventType:       "VIEW_FINISHED",
		Timestamp:       eventTime.Add(2 * time.Minute).Format(time.RFC3339),
		DeviceType:      "MOBILE",
		SessionID:       sessionID,
		ProgressSeconds: 120,
	})

	waitForClickHouseEvents(t, ctx, ch, startedEventID, finishedEventID)

	aggregate := runAggregation(t, date)
	if aggregate.Status != "ok" {
		t.Fatalf("expected aggregate status ok, got %q", aggregate.Status)
	}
	if aggregate.Date != date {
		t.Fatalf("expected aggregate date %s, got %s", date, aggregate.Date)
	}
	if aggregate.Records < 2 {
		t.Fatalf("expected at least 2 processed records, got %.0f", aggregate.Records)
	}
	if aggregate.DAU < 1 {
		t.Fatalf("expected at least 1 DAU, got %.0f", aggregate.DAU)
	}

	assertPostgresMetricAtLeast(t, pg, date, "dau", 1)
	assertPostgresMetricAtLeast(t, pg, date, "view_started", 1)
	assertPostgresMetricAtLeast(t, pg, date, "view_finished", 1)
}

func openClickHouse(t *testing.T) clickhouse.Conn {
	t.Helper()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "analytics",
			Password: "analytics",
		},
	})
	if err != nil {
		t.Fatalf("connect clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}
	return conn
}

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

func cleanupTestData(t *testing.T, ctx context.Context, ch clickhouse.Conn, pg *sql.DB, date string, suffix string) {
	t.Helper()

	if err := ch.Exec(ctx, "ALTER TABLE raw_events DELETE WHERE endsWith(event_id, ?)", suffix); err != nil {
		t.Logf("cleanup clickhouse raw_events failed: %v", err)
	}
	if _, err := pg.ExecContext(ctx, "DELETE FROM daily_metrics WHERE date = $1", date); err != nil {
		t.Logf("cleanup postgres daily_metrics failed: %v", err)
	}
}

func postEvent(t *testing.T, event eventRequest) {
	t.Helper()

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	resp, err := http.Post(producerEventsURL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post event: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read producer response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected producer status 200, got %d: %s", resp.StatusCode, data)
	}

	var parsed producerResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("decode producer response: %v: %s", err, data)
	}
	if parsed.Status != "accepted" {
		t.Fatalf("expected accepted producer response, got %q", parsed.Status)
	}
	if parsed.EventID != event.EventID {
		t.Fatalf("expected event id %q, got %q", event.EventID, parsed.EventID)
	}
}

func waitForClickHouseEvents(t *testing.T, ctx context.Context, ch clickhouse.Conn, eventIDs ...string) {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	for {
		var count uint64
		err := ch.QueryRow(
			ctx,
			"SELECT count() FROM raw_events WHERE event_id IN (?, ?)",
			eventIDs[0],
			eventIDs[1],
		).Scan(&count)
		if err == nil && count == uint64(len(eventIDs)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("events did not reach clickhouse, count=%d, err=%v", count, err)
		}
		time.Sleep(time.Second)
	}
}

func runAggregation(t *testing.T, date string) aggregateResponse {
	t.Helper()

	resp, err := http.Get(aggregatorURL + "?date=" + date)
	if err != nil {
		t.Fatalf("run aggregation: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read aggregate response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected aggregate status 200, got %d: %s", resp.StatusCode, data)
	}

	var parsed aggregateResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("decode aggregate response: %v: %s", err, data)
	}
	return parsed
}

func assertPostgresMetricAtLeast(t *testing.T, db *sql.DB, date string, metricName string, minValue float64) {
	t.Helper()

	var value float64
	err := db.QueryRow(
		"SELECT metric_value::float8 FROM daily_metrics WHERE date = $1 AND metric_name = $2",
		date,
		metricName,
	).Scan(&value)
	if err != nil {
		t.Fatalf("read postgres metric %s: %v", metricName, err)
	}
	if value < minValue {
		t.Fatalf("expected %s >= %.2f, got %.2f", metricName, minValue, value)
	}
}

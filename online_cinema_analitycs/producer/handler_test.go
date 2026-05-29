package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestValidateEventRequestAcceptsViewEvent(t *testing.T) {
	progress := 42
	req := EventRequest{
		UserID:          "user_1",
		MovieID:         "movie_1",
		EventType:       "VIEW_STARTED",
		Timestamp:       time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
		DeviceType:      "MOBILE",
		SessionID:       "session_1",
		ProgressSeconds: &progress,
	}

	if err := validateEventRequest(&req); err != nil {
		t.Fatalf("validateEventRequest returned error: %v", err)
	}

	if req.EventID == "" {
		t.Fatal("expected validateEventRequest to generate event id")
	}
}

func TestValidateEventRequestRejectsInvalidEventType(t *testing.T) {
	req := EventRequest{
		UserID:     "user_1",
		EventType:  "UNKNOWN",
		DeviceType: "MOBILE",
	}

	if err := validateEventRequest(&req); err == nil {
		t.Fatal("expected invalid event type error")
	}
}

func TestValidateEventRequestRejectsMissingViewFields(t *testing.T) {
	req := EventRequest{
		UserID:     "user_1",
		EventType:  "VIEW_FINISHED",
		DeviceType: "TV",
	}

	if err := validateEventRequest(&req); err == nil {
		t.Fatal("expected missing view fields error")
	}
}

func TestValidateEventRequestClearsLikeProgress(t *testing.T) {
	progress := 10
	req := EventRequest{
		UserID:          "user_1",
		MovieID:         "movie_1",
		EventType:       "LIKED",
		DeviceType:      "DESKTOP",
		ProgressSeconds: &progress,
	}

	if err := validateEventRequest(&req); err != nil {
		t.Fatalf("validateEventRequest returned error: %v", err)
	}
	if req.ProgressSeconds != nil {
		t.Fatal("expected LIKED event progress to be cleared")
	}
}

func TestEventsHandlerReturnsBadRequestForInvalidJSON(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader("{bad json"))
	response := httptest.NewRecorder()

	EventsHandler(nil).ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, response.Code)
	}
	if !strings.Contains(response.Body.String(), "invalid JSON") {
		t.Fatalf("expected invalid JSON response, got %q", response.Body.String())
	}
}

func TestSplitBrokers(t *testing.T) {
	got := splitBrokers("kafka-1:29092,kafka-2:29092")
	want := []string{"kafka-1:29092", "kafka-2:29092"}

	if len(got) != len(want) {
		t.Fatalf("expected %d brokers, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("broker %d: expected %q, got %q", i, want[i], got[i])
		}
	}
}

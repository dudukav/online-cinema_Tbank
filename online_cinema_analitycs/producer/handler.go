package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func EventsHandler(kp *KafkaProducer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req EventRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error": "invalid JSON"}`, http.StatusBadRequest)
			return
		}

		if err := validateEventRequest(&req); err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		event := KafkaEvent{
			EventID:         req.EventID,
			UserID:          req.UserID,
			MovieID:         req.MovieID,
			EventType:       req.EventType,
			TimestampMs:     req.Timestamp.UTC().UnixMilli(),
			DeviceType:      req.DeviceType,
			SessionID:       req.SessionID,
			ProgressSeconds: progressValue(req.ProgressSeconds),
		}

		if err := kp.ProduceEvent(r.Context(), event); err != nil {
			http.Error(w, `{"error": "kafka produce failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"event_id":      req.EventID,
			"status":        "accepted",
			"timestamp_utc": req.Timestamp.Format(time.RFC3339),
		})
	}
}

func validateEventRequest(req *EventRequest) error {
	if req.EventID == "" {
		req.EventID = newID()
	}
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now().UTC()
	}
	if req.UserID == "" {
		return fmt.Errorf("user_id required")
	}
	if !isValidDeviceType(req.DeviceType) {
		return fmt.Errorf("invalid device_type")
	}

	switch req.EventType {
	case "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED":
		if req.MovieID == "" || req.SessionID == "" || req.ProgressSeconds == nil {
			return fmt.Errorf("movie_id, session_id, progress_seconds required for view events")
		}
		if *req.ProgressSeconds < 0 {
			return fmt.Errorf("progress_seconds must be non-negative")
		}
	case "LIKED":
		if req.MovieID == "" {
			return fmt.Errorf("movie_id required for LIKED event")
		}
		if req.ProgressSeconds != nil {
			req.ProgressSeconds = nil
		}
	case "SEARCHED":
		if req.DeviceType == "" {
			return fmt.Errorf("device_type required for SEARCHED event")
		}
	default:
		return fmt.Errorf("invalid event_type")
	}
	return nil
}

func isViewEvent(eventType string) bool {
	switch eventType {
	case "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED":
		return true
	default:
		return false
	}
}

func isValidDeviceType(deviceType string) bool {
	switch deviceType {
	case "MOBILE", "DESKTOP", "TV", "TABLET":
		return true
	default:
		return false
	}
}

func progressValue(progress *int) int {
	if progress == nil {
		return 0
	}
	return *progress
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

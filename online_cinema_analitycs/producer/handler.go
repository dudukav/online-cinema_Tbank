package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	pb "producer/proto/schema"
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

		pbEvent := &pb.MovieEvent{
			EventId:        req.EventId,
			UserId:         req.UserId,
			MovieId:        req.MovieId,
			SessionId:      req.SessionId,
			TimestampMs:    req.Timestamp.UnixMilli(),
			DeviceType:     pb.DeviceType(pb.DeviceType_value[req.DeviceType]),
			ProgressSeconds: int32(*req.ProgressSeconds),
		}

		if et, ok := pb.EventType_value[req.EventType]; ok {
			pbEvent.EventType = pb.EventType(et)
		} else {
			http.Error(w, `{"error": "invalid event_type"}`, http.StatusBadRequest)
			return
		}

		if err := kp.ProduceEvent(r.Context(), pbEvent); err != nil {
			http.Error(w, `{"error": "kafka produce failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"event_id":      req.EventId,
			"status":        "accepted",
			"timestamp_utc": req.Timestamp.Format(time.RFC3339),
		})
	}
}

func validateEventRequest(req *EventRequest) error {
	switch req.EventType {
	case "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED":
		if req.MovieId == "" || req.SessionId == "" || req.ProgressSeconds == nil {
			return fmt.Errorf("movie_id, session_id, progress_seconds required for view events")
		}
	case "LIKED":
		if req.MovieId == "" {
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
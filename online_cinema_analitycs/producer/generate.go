package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	pb "producer/proto/schema"
)

func EventsGenerateHandler(kp *KafkaProducer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for user := 0; user < 10; user++ {
			userId := fmt.Sprintf("user_%d", user)
			movieId := "movie_123"
			sessionId := fmt.Sprintf("sess_%d", user)
			startTime := time.Now().Add(-1 * time.Hour).Add(time.Duration(user) * 10 * time.Minute)

			generateViewSequence(r.Context(), kp, userId, movieId, sessionId, startTime)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "generated 10 view sequences",
		})
	}
}

func generateViewSequence(
	ctx context.Context,
	kp *KafkaProducer,
	userId, movieId, sessionId string,
	startTime time.Time,
) {
	ts := startTime

	ev1 := &pb.MovieEvent{}
	ev1.EventType = pb.EventType_VIEW_STARTED
	ev1.ProgressSeconds = 0
	kp.ProduceEvent(ctx, ev1)

	ts = ts.Add(10 * time.Minute)
	ev2 := cloneEvent(ev1)
	ev2.EventType = pb.EventType_VIEW_PAUSED
	ev2.ProgressSeconds = 600
	kp.ProduceEvent(ctx, ev2)

	ts = ts.Add(5 * time.Minute)
	ev3 := cloneEvent(ev1)
	ev3.EventType = pb.EventType_VIEW_RESUMED
	ev3.ProgressSeconds = 650
	kp.ProduceEvent(ctx, ev3)

	ts = ts.Add(40 * time.Minute)
	ev4 := cloneEvent(ev1)
	ev4.EventType = pb.EventType_VIEW_FINISHED
	ev4.ProgressSeconds = 3000
	kp.ProduceEvent(ctx, ev4)
}

func cloneEvent(ev *pb.MovieEvent) *pb.MovieEvent {
	return &pb.MovieEvent{
		EventId:         ev.EventId,
		UserId:          ev.UserId,
		MovieId:         ev.MovieId,
		SessionId:       ev.SessionId,
		EventType:       ev.EventType,
		TimestampMs:     ev.TimestampMs,
		DeviceType:      ev.DeviceType,
		ProgressSeconds: ev.ProgressSeconds,
	}
}
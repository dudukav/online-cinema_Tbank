package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
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
		for user := 0; user < 3; user++ {
			userId := fmt.Sprintf("retention_d1_user_%d", user)
			generateViewSequence(r.Context(), kp, userId, "movie_retention", fmt.Sprintf("ret_d1_sess_%d", user), time.Now().AddDate(0, 0, -1))
			generateSearchEvent(r.Context(), kp, userId, time.Now())
		}
		for user := 0; user < 2; user++ {
			userId := fmt.Sprintf("retention_d7_user_%d", user)
			generateViewSequence(r.Context(), kp, userId, "movie_retention", fmt.Sprintf("ret_d7_sess_%d", user), time.Now().AddDate(0, 0, -7))
			generateSearchEvent(r.Context(), kp, userId, time.Now())
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "generated demo view and retention sequences",
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
	device := []string{"MOBILE", "DESKTOP", "TV", "TABLET"}[rand.Intn(4)]

	ev1 := KafkaEvent{
		EventID:         newID(),
		UserID:          userId,
		MovieID:         movieId,
		SessionID:       sessionId,
		EventType:       "VIEW_STARTED",
		TimestampMs:     ts.UTC().UnixMilli(),
		DeviceType:      device,
		ProgressSeconds: 0,
	}
	kp.ProduceEvent(ctx, ev1)

	ts = ts.Add(10 * time.Minute)
	ev2 := cloneEvent(ev1)
	ev2.EventID = newID()
	ev2.EventType = "VIEW_PAUSED"
	ev2.TimestampMs = ts.UTC().UnixMilli()
	ev2.ProgressSeconds = 600
	kp.ProduceEvent(ctx, ev2)

	ts = ts.Add(5 * time.Minute)
	ev3 := cloneEvent(ev1)
	ev3.EventID = newID()
	ev3.EventType = "VIEW_RESUMED"
	ev3.TimestampMs = ts.UTC().UnixMilli()
	ev3.ProgressSeconds = 650
	kp.ProduceEvent(ctx, ev3)

	ts = ts.Add(40 * time.Minute)
	ev4 := cloneEvent(ev1)
	ev4.EventID = newID()
	ev4.EventType = "VIEW_FINISHED"
	ev4.TimestampMs = ts.UTC().UnixMilli()
	ev4.ProgressSeconds = 3000
	kp.ProduceEvent(ctx, ev4)

	like := cloneEvent(ev1)
	like.EventID = newID()
	like.EventType = "LIKED"
	like.TimestampMs = ts.Add(2 * time.Minute).UTC().UnixMilli()
	like.ProgressSeconds = 0
	kp.ProduceEvent(ctx, like)
}

func cloneEvent(ev KafkaEvent) KafkaEvent {
	return ev
}

func generateSearchEvent(ctx context.Context, kp *KafkaProducer, userId string, ts time.Time) {
	event := KafkaEvent{
		EventID:         newID(),
		UserID:          userId,
		EventType:       "SEARCHED",
		TimestampMs:     ts.UTC().UnixMilli(),
		DeviceType:      []string{"MOBILE", "DESKTOP", "TV", "TABLET"}[rand.Intn(4)],
		ProgressSeconds: 0,
	}
	kp.ProduceEvent(ctx, event)
}

package main

import "time"

type EventRequest struct {
	EventID         string    `json:"event_id"`
	UserID          string    `json:"user_id"`
	MovieID         string    `json:"movie_id"`
	EventType       string    `json:"event_type"`
	Timestamp       time.Time `json:"timestamp"`
	DeviceType      string    `json:"device_type"`
	SessionID       string    `json:"session_id"`
	ProgressSeconds *int      `json:"progress_seconds,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

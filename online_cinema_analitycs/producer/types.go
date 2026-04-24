package main

import "time"

type EventRequest struct {
	EventId        string      `json:"event_id"`
	UserId         string      `json:"user_id"`
	MovieId        string      `json:"movie_id"`
	EventType      string      `json:"event_type"`
	Timestamp      time.Time   `json:"timestamp"`
	DeviceType     string      `json:"device_type"`
	SessionId      string      `json:"session_id"`
	ProgressSeconds *int       `json:"progress_seconds,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
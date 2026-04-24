package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	kp, err := NewKafkaProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", EventsHandler(kp))
	mux.HandleFunc("/events/generate", EventsGenerateHandler(kp))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting producer on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
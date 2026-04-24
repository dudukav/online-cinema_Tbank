package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func StartScheduler(interval time.Duration, dateToProcess func() time.Time, ch clickhouse.Conn, psql *sql.DB) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		date := dateToProcess()
		log.Printf("Start aggregation for date %s", date.Format("2006-01-02"))
		start := time.Now()

		err := aggregateForDate(date, ch, psql)
		if err != nil {
			log.Printf("Error aggregating for %s: %v", date, err)
			continue
		}

		duration := time.Since(start)
		log.Printf("Aggregation for %s finished. Took %.2f seconds.",
			date, duration.Seconds())
	}
}
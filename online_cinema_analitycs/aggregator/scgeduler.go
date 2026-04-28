package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func StartScheduler(interval time.Duration, dateToProcess func() time.Time, ch clickhouse.Conn, psql *sql.DB) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		date := dateToProcess()
		log.Printf("aggregation started date=%s", date.Format("2006-01-02"))
		if _, err := aggregateForDate(date, ch, psql); err != nil {
			log.Printf("aggregation failed date=%s error=%v", date.Format("2006-01-02"), err)
		}
	}
}

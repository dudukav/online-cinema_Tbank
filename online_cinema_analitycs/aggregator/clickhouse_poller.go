package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

var ch clickhouse.Conn
var psql *sql.DB

type RetentionHit struct {
	DaysSince int    `clickhouse:"days_since"`
	Count     uint64 `clickhouse:"returning_users"`
}

type TopMovieHit struct {
	MovieID string
	Views   uint64
}

func aggregateForDate(date time.Time, ch clickhouse.Conn, psql *sql.DB) error {
	dateStr := date.Format("2006-01-02")

	row := ch.QueryRow(
		context.Background(),
		"SELECT dau, avg_view_time, conversion FROM day_metrics WHERE date = $1",
		dateStr,
	)
	var dau uint64
	var avgTime float64
	var conv float64
	err := row.Scan(&dau, &avgTime, &conv)
	if err != nil {
		return err
	}

	retentionHits := make(map[int]uint64)
	rows, err := ch.Query(
		context.Background(),
		"SELECT days_since, returning_users FROM retention_cohorts WHERE cohort_date = $1",
		dateStr,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var r RetentionHit
		if err := rows.Scan(&r.DaysSince, &r.Count); err != nil {
			return err
		}
		retentionHits[r.DaysSince] = r.Count
	}

	topMovies := make(map[string]uint64)
	rows, err = ch.Query(
		context.Background(),
		"SELECT movie_id, views FROM top_movies_daily WHERE date = $1",
		dateStr,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var m TopMovieHit
		if err := rows.Scan(&m.MovieID, &m.Views); err != nil {
			return err
		}
		topMovies[m.MovieID] = m.Views
	}

	// 4. Передаём все данные в PostgreSQL
	return writeToPostgres(date, dau, avgTime, conv, retentionHits, topMovies)
}
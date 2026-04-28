package main

import (
	"database/sql"
	"fmt"
	"time"
)

func writeToPostgres(psql *sql.DB, result AggregationResult) error {
	var err error
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		err = writeToPostgresOnce(psql, result)
		if err == nil {
			return nil
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return err
}

func writeToPostgresOnce(psql *sql.DB, result AggregationResult) error {
	tx, err := psql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	metrics := map[string]float64{
		"DAU":               float64(result.DAU),
		"avg_view_time":     result.AvgViewTime,
		"view_started":      float64(result.ViewStarted),
		"view_finished":     float64(result.ViewFinished),
		"conversion":        result.Conversion,
		"processed_records": float64(result.ProcessedRecords),
	}

	for day, retention := range result.Retention {
		if day == 1 || day == 7 {
			metrics[fmt.Sprintf("retention_D%d", day)] = retention.Retention
			metrics[fmt.Sprintf("retention_D%d_users", day)] = float64(retention.ReturningUsers)
		}
	}

	for _, movie := range result.TopMovies {
		metrics[fmt.Sprintf("top_movie_views:%s", movie.MovieID)] = float64(movie.Views)
	}

	for name, value := range metrics {
		if _, err := tx.Exec(
			`INSERT INTO daily_metrics (date, metric_name, metric_value, computed_at)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (date, metric_name) DO UPDATE
			 SET metric_value = EXCLUDED.metric_value,
			     computed_at = EXCLUDED.computed_at`,
			result.Date,
			name,
			value,
			now,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

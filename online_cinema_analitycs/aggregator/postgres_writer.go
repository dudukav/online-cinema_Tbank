package main

import (
	"fmt"
	"time"
)

func writeToPostgres(
	date time.Time,
	dau uint64,
	avgTime float64,
	conv float64,
	retentionHits map[int]uint64,
	topMovies map[string]uint64,
) error {
	tx, err := psql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now()

	_, err = tx.Exec(
		`INSERT INTO daily_metrics (date, metric_name, metric_value, computed_at)
		 VALUES ($1, 'DAU', $2, $3)
		 ON CONFLICT (date, metric_name) DO UPDATE
		 SET metric_value = EXCLUDED.metric_value, computed_at = EXCLUDED.computed_at`,
		date, dau, now,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`INSERT INTO daily_metrics (date, metric_name, metric_value, computed_at)
		 VALUES ($1, 'avg_view_time', $2, $3)
		 ON CONFLICT (date, metric_name) DO UPDATE
		 SET metric_value = EXCLUDED.metric_value, computed_at = EXCLUDED.computed_at`,
		date, avgTime, now,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`INSERT INTO daily_metrics (date, metric_name, metric_value, computed_at)
		 VALUES ($1, 'conversion', $2, $3)
		 ON CONFLICT (date, metric_name) DO UPDATE
		 SET metric_value = EXCLUDED.metric_value, computed_at = EXCLUDED.computed_at`,
		date, conv, now,
	)
	if err != nil {
		return err
	}

	// 4. Retention D1, D7
	for days, count := range retentionHits {
		if days != 1 && days != 7 {
			continue
		}
		name := fmt.Sprintf("retention_D%d", days)
		_, err = tx.Exec(
			`INSERT INTO daily_metrics (date, metric_name, metric_value, computed_at)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (date, metric_name) DO UPDATE
			 SET metric_value = EXCLUDED.metric_value, computed_at = EXCLUDED.computed_at`,
			date, name, float64(count), now,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
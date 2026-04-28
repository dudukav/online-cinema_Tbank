package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type AggregationResult struct {
	Date             time.Time
	ProcessedRecords uint64
	DAU              uint64
	AvgViewTime      float64
	ViewStarted      uint64
	ViewFinished     uint64
	Conversion       float64
	Retention        map[int]RetentionMetric
	TopMovies        []TopMovieMetric
}

type RetentionMetric struct {
	DaysSince      int
	CohortSize     uint64
	ReturningUsers uint64
	Retention      float64
}

type TopMovieMetric struct {
	MovieID string
	Views   uint64
}

func aggregateForDate(date time.Time, ch clickhouse.Conn, psql *sql.DB) (AggregationResult, error) {
	day := date.UTC().Format("2006-01-02")
	start := time.Now()

	if err := materializeClickHouseAggregates(day, ch); err != nil {
		return AggregationResult{}, err
	}

	result, err := readMaterializedAggregates(day, ch)
	if err != nil {
		return AggregationResult{}, err
	}

	if err := writeToPostgres(psql, result); err != nil {
		return AggregationResult{}, err
	}

	log.Printf(
		"aggregation finished date=%s processed_records=%d dau=%d conversion=%.4f duration=%s",
		day,
		result.ProcessedRecords,
		result.DAU,
		result.Conversion,
		time.Since(start).Round(time.Millisecond),
	)
	return result, nil
}

func materializeClickHouseAggregates(day string, ch clickhouse.Conn) error {
	ctx := context.Background()
	statements := []string{
		fmt.Sprintf("ALTER TABLE day_metrics DELETE WHERE date = toDate('%s')", day),
		fmt.Sprintf("ALTER TABLE top_movies_daily DELETE WHERE date = toDate('%s')", day),
		fmt.Sprintf("ALTER TABLE retention_cohorts DELETE WHERE cohort_date = toDate('%s')", day),
		fmt.Sprintf(`
			INSERT INTO day_metrics
			SELECT
				toDate('%[1]s') AS date,
				dau,
				avg_view_time,
				view_started,
				view_finished,
				if(view_started = 0, 0, view_finished / view_started) AS conversion,
				now() AS computed_at
			FROM
			(
				SELECT
					uniqExact(user_id) AS dau,
					if(isNaN(avgIf(toFloat64(progress_seconds), event_type = 'VIEW_FINISHED')), 0, avgIf(toFloat64(progress_seconds), event_type = 'VIEW_FINISHED')) AS avg_view_time,
					countIf(event_type = 'VIEW_STARTED') AS view_started,
					countIf(event_type = 'VIEW_FINISHED') AS view_finished
				FROM raw_events
				WHERE date = toDate('%[1]s')
			)`, day),
		fmt.Sprintf(`
			INSERT INTO top_movies_daily
			SELECT
				toDate('%[1]s') AS date,
				movie_id,
				count() AS views,
				now() AS computed_at
			FROM raw_events
			WHERE raw_events.date = toDate('%[1]s')
				AND event_type = 'VIEW_STARTED'
				AND movie_id != ''
			GROUP BY movie_id
			ORDER BY views DESC
			LIMIT 10`, day),
		fmt.Sprintf(`
			INSERT INTO retention_cohorts
			WITH
				cohort AS
				(
					SELECT user_id, min(date) AS cohort_date
					FROM raw_events
					GROUP BY user_id
					HAVING cohort_date = toDate('%[1]s')
				),
				size AS
				(
					SELECT count() AS cohort_size FROM cohort
				)
			SELECT
				toDate('%[1]s') AS cohort_date,
				toUInt8(dateDiff('day', cohort.cohort_date, raw_events.date)) AS days_since,
				any(size.cohort_size) AS cohort_size,
				uniqExact(raw_events.user_id) AS returning_users,
				if(any(size.cohort_size) = 0, 0, returning_users / any(size.cohort_size)) AS retention,
				now() AS computed_at
			FROM raw_events
			INNER JOIN cohort ON raw_events.user_id = cohort.user_id
			CROSS JOIN size
			WHERE raw_events.date BETWEEN toDate('%[1]s') AND addDays(toDate('%[1]s'), 7)
			GROUP BY days_since
			ORDER BY days_since`, day),
	}

	for _, statement := range statements {
		if err := ch.Exec(ctx, statement); err != nil {
			return err
		}
	}

	return nil
}

func readMaterializedAggregates(day string, ch clickhouse.Conn) (AggregationResult, error) {
	ctx := context.Background()
	result := AggregationResult{
		Retention: make(map[int]RetentionMetric),
	}

	if err := ch.QueryRow(ctx, `
		SELECT
			date,
			(
				SELECT count()
				FROM raw_events
				WHERE date = toDate(?)
			) AS processed_records,
			dau,
			avg_view_time,
			view_started,
			view_finished,
			conversion
		FROM day_metrics
		WHERE date = toDate(?)
		ORDER BY computed_at DESC
		LIMIT 1`,
		day,
		day,
	).Scan(
		&result.Date,
		&result.ProcessedRecords,
		&result.DAU,
		&result.AvgViewTime,
		&result.ViewStarted,
		&result.ViewFinished,
		&result.Conversion,
	); err != nil {
		return AggregationResult{}, err
	}

	topRows, err := ch.Query(ctx, `
		SELECT movie_id, views
		FROM top_movies_daily
		WHERE date = toDate(?)
		ORDER BY views DESC, movie_id
		LIMIT 10`, day)
	if err != nil {
		return AggregationResult{}, err
	}
	defer topRows.Close()

	for topRows.Next() {
		var metric TopMovieMetric
		if err := topRows.Scan(&metric.MovieID, &metric.Views); err != nil {
			return AggregationResult{}, err
		}
		result.TopMovies = append(result.TopMovies, metric)
	}
	if err := topRows.Err(); err != nil {
		return AggregationResult{}, err
	}

	retentionRows, err := ch.Query(ctx, `
		SELECT days_since, cohort_size, returning_users, retention
		FROM retention_cohorts
		WHERE cohort_date = toDate(?)
		ORDER BY days_since`, day)
	if err != nil {
		return AggregationResult{}, err
	}
	defer retentionRows.Close()

	for retentionRows.Next() {
		var metric RetentionMetric
		var daysSince uint8
		if err := retentionRows.Scan(&daysSince, &metric.CohortSize, &metric.ReturningUsers, &metric.Retention); err != nil {
			return AggregationResult{}, err
		}
		metric.DaysSince = int(daysSince)
		result.Retention[metric.DaysSince] = metric
	}
	if err := retentionRows.Err(); err != nil {
		return AggregationResult{}, err
	}

	return result, nil
}

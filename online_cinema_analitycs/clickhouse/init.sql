CREATE TABLE IF NOT EXISTS kafka_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       String,
    timestamp_ms     Int64,
    device_type      String,
    session_id       String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:29092',
    kafka_topic_list  = 'movie-events',
    kafka_group_name  = 'clickhouse_consumer_group',
    kafka_format      = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS raw_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    timestamp_ms     Int64,
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Int32,
    event_date       DateTime MATERIALIZED fromUnixTimestamp64Milli(timestamp_ms),
    date             Date MATERIALIZED toDate(event_date)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, user_id, session_id, timestamp_ms, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_to_raw
TO raw_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp_ms,
    device_type,
    session_id,
    progress_seconds
FROM kafka_events;

CREATE TABLE IF NOT EXISTS day_metrics
(
    date          Date,
    dau           UInt64,
    avg_view_time Float64,
    view_started  UInt64,
    view_finished UInt64,
    conversion    Float64,
    computed_at   DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE IF NOT EXISTS top_movies_daily
(
    date        Date,
    movie_id    String,
    views       UInt64,
    computed_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, views, movie_id);

CREATE TABLE IF NOT EXISTS retention_cohorts
(
    cohort_date     Date,
    days_since      UInt8,
    cohort_size     UInt64,
    returning_users UInt64,
    retention       Float64,
    computed_at     DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, days_since);

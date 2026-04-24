CREATE TABLE IF NOT EXISTS kafka_events (
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
    kafka_broker_list = 'kafka-1:9092',
    kafka_topic_list  = 'movie-events',
    kafka_group_name  = 'clickhouse_consumer_group',
    kafka_format      = 'Protobuf',
    kafka_schema      = 'movie.cinema.MovieEvent';

CREATE TABLE IF NOT EXISTS raw_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       Enum8
        ('VIEW_STARTED'   = 1,
         'VIEW_FINISHED'  = 2,
         'VIEW_PAUSED'    = 3,
         'VIEW_RESUMED'   = 4,
         'LIKED'          = 5,
         'SEARCHED'       = 6),
    timestamp_ms     Int64,
    device_type      Enum8
        ('MOBILE'  = 1,
         'DESKTOP' = 2,
         'TV'      = 3,
         'TABLET'  = 4),
    session_id       String,
    progress_seconds Int32,

    event_date       Date     DEFAULT toDate(fromUnixTimestamp64Milli(timestamp_ms)),
    event_datetime   DateTime DEFAULT fromUnixTimestamp64Milli(timestamp_ms)
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (user_id, event_datetime)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_to_raw
TO raw_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    toEnum8(event_type, 
        'VIEW_STARTED', 'VIEW_FINISHED', 'VIEW_PAUSED', 'VIEW_RESUMED', 'LIKED', 'SEARCHED'
    ) AS event_type,
    timestamp_ms,
    toEnum8(device_type, 'MOBILE', 'DESKTOP', 'TV', 'TABLET') AS device_type,
    session_id,
    progress_seconds
FROM kafka_events;

CREATE TABLE IF NOT EXISTS day_metrics (
    date            Date,
    dau             UInt64,
    avg_view_time   Float64,
    view_started    UInt64,
    view_finished   UInt64,
    conversion      Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date);

CREATE TABLE IF NOT EXISTS top_movies_daily (
    date            Date,
    movie_id        String,
    views           UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, views DESC);

CREATE TABLE IF NOT EXISTS retention_cohorts (
    cohort_date     Date,
    days_since      UInt8,
    returning_users UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, days_since);
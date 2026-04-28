CREATE TABLE IF NOT EXISTS daily_metrics (
    date            DATE,
    metric_name     VARCHAR(128),
    metric_value    NUMERIC(18, 6),
    computed_at     TIMESTAMP,
    UNIQUE(date, metric_name)
);

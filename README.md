# Online Cinema Analytics Pipeline

Event streaming и аналитический pipeline для онлайн-кинотеатра на базе **Kafka**, **Schema Registry**, **ClickHouse**, **PostgreSQL** и **Grafana**.

Проект моделирует обработку пользовательских событий онлайн-кинотеатра: просмотры, паузы, лайки, поисковые запросы и завершённые просмотры. События публикуются в Kafka, проходят через ingestion pipeline в ClickHouse, агрегируются отдельным сервисом и сохраняются в PostgreSQL для дальнейшей визуализации и экспорта.

## Стек

- Kafka
- Schema Registry
- ClickHouse
- PostgreSQL
- Docker Compose
- <Golang>
- <Kafka / ClickHouse / PostgreSQL>

## Архитектура

```text
Movie Service
  ├── HTTP API
  └── Synthetic Event Generator
        │
        ▼
Kafka topic: movie-events
        │
        ▼
Schema Registry
        │
        ▼
ClickHouse Kafka Engine
        │
        ▼
ClickHouse MergeTree raw events
        │
        ▼
Aggregation Service
        │
        ▼
PostgreSQL metrics
```

## Возможности

- Ingestion pipeline
- Приём событий через HTTP API
- Валидация событий перед отправкой в Kafka
- Публикация событий в Kafka topic movie-events
- Использование Schema Registry для версионирования схемы событий
- Kafka topic с несколькими партициями
- Партиционирование событий по user_id для сохранения порядка пользовательских действий
- Retry с exponential backoff при ошибках публикации
- Логирование опубликованных событий
- Генератор событий

Сервис умеет генерировать синтетические пользовательские сценарии:

- VIEW_STARTED
- VIEW_PAUSED
- VIEW_RESUMED
- VIEW_FINISHED
- LIKED
- SEARCHED

Генератор создаёт реалистичные последовательности событий внутри одной сессии:

VIEW_STARTED → VIEW_PAUSED → VIEW_RESUMED → VIEW_FINISHED

При этом progress_seconds увеличивается по мере просмотра.

## Событие

События описаны через <Protobuf> и регистрируются в Schema Registry.

Пример события:

```JSON
{
  "event_id": "9f4a75c0-6f5f-4e4a-9d7d-2d2cf44c0e21",
  "user_id": "user-123",
  "movie_id": "movie-456",
  "event_type": "VIEW_FINISHED",
  "timestamp": "2026-04-28T10:00:00Z",
  "device_type": "MOBILE",
  "session_id": "session-789",
  "progress_seconds": 5420
}
```

## Kafka

Используется topic:

movie-events

Topic создаётся минимум с 3 партициями.

В качестве ключа партиционирования используется user_id, чтобы все события одного пользователя попадали в одну партицию и сохраняли порядок действий внутри пользовательской сессии.

## ClickHouse

Для ingestion используются две таблицы:

Kafka Engine table — читает события напрямую из Kafka.
MergeTree table — хранит raw-события постоянно.

Перенос данных из Kafka Engine table в MergeTree table выполняется автоматически через Materialized View.

Raw-события хранятся в ClickHouse и используются как источник для аналитических расчётов.

Aggregation Service

Отдельный сервис агрегации:

Работает в отдельном контейнере
Читает raw-события напрямую из ClickHouse
Запускает пересчёт метрик по расписанию
Поддерживает ручной запуск пересчёта через HTTP endpoint
Логирует начало и завершение каждого цикла
Сохраняет готовые метрики в PostgreSQL

Интервал пересчёта настраивается через переменную окружения:

AGGREGATION_INTERVAL=5m

## Бизнес-метрики

Сервис считает дневные агрегаты:

- Метрика	Описание
- DAU	Количество уникальных пользователей за день
- Average watch time	Средний progress_seconds для VIEW_FINISHED
- Top movies	Рейтинг фильмов по количеству просмотров
- View conversion	Доля VIEW_FINISHED от VIEW_STARTED
- Retention D1	Доля пользователей, вернувшихся через 1 день
- Retention D7	Доля пользователей, вернувшихся через 7 дней

Для расчётов используются агрегатные функции ClickHouse: uniq, avg, countIf, sum, group by.

## PostgreSQL

PostgreSQL используется как хранилище готовых метрик.

Минимальная структура таблицы:

```SQL
CREATE TABLE metrics (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    calculated_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name)
);
```

Запись метрик идемпотентная: повторный пересчёт за ту же дату обновляет существующие значения, а не создаёт дубли.

## Миграции

Миграции ClickHouse и PostgreSQL применяются автоматически при старте контейнеров.

## Тесты

Интеграционный тест проверяет полный путь события:

HTTP API → Kafka → ClickHouse Kafka Engine → MergeTree table

## Структура проекта
```
.
├── Makefile
├── aggregator
│   ├── Dockerfile
│   ├── clickhouse_poller.go
│   ├── handler.go
│   ├── main.go
│   ├── postgres_writer.go
│   └── scgeduler.go
├── clickhouse
│   └── init.sql
├── docker-compose.yml
├── postgres
│   └── init.sql
├── producer
│   ├── Dockerfile
│   ├── generate.go
│   ├── handler.go
│   ├── kafka_producer.go
│   ├── main.go
│   ├── tests
│   │   └── integration_test.go
│   └── types.go
└── schema
    ├── movie_event.proto
    └── movie_event.schema.json
```

# Команды для запуска и проверки

```bash
cd online_cinema_analitycs
docker compose up -d --build
docker compose ps
```

## Что проверить после запуска

### 1. Kafka topic

```bash
docker compose exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe --topic movie-events
```

### 2. Schema Registry

```bash
curl http://127.0.0.1:8081/subjects
curl http://127.0.0.1:8081/subjects/movie-events-value/versions
```

### 3. Producer API

```bash
curl -X POST http://127.0.0.1:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id":"demo-event-1",
    "user_id":"user-demo",
    "movie_id":"movie-42",
    "event_type":"VIEW_STARTED",
    "timestamp":"2026-04-28T12:00:00Z",
    "device_type":"MOBILE",
    "session_id":"session-demo",
    "progress_seconds":0
  }'
```

### 4. Synthetic generator

```bash
curl -X POST http://127.0.0.1:8080/events/generate
```

### 5. ClickHouse raw events

```bash
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics \
  --query "SELECT event_id, user_id, movie_id, event_type, device_type, progress_seconds FROM raw_events ORDER BY timestamp_ms DESC LIMIT 10"
```

### 6. Manual aggregation

```bash
curl "http://127.0.0.1:8082/aggregate?date=2026-04-28"
```

```bash
curl "http://127.0.0.1:8082/aggregate?date=$(date +%F)"
```

```bash
curl "http://127.0.0.1:8082/aggregate?date=$(date -v-1d +%F)" 
curl "http://127.0.0.1:8082/aggregate?date=$(date -v-7d +%F)"
```

### 7. ClickHouse materialized aggregates

```bash
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics \
  --query "SELECT * FROM day_metrics ORDER BY computed_at DESC LIMIT 5"

docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics \
  --query "SELECT * FROM top_movies_daily ORDER BY computed_at DESC, views DESC LIMIT 10"

docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics \
  --query "SELECT * FROM retention_cohorts ORDER BY cohort_date DESC, days_since LIMIT 20"
```

### 8. PostgreSQL готовые метрики

```bash
docker compose exec postgres psql -U user -d analytics \
  -c "SELECT * FROM daily_metrics ORDER BY date DESC, metric_name;"
```

### 9. Integration test

```bash
cd producer
RUN_INTEGRATION_TESTS=1 go test ./tests -v
```

## Соответствие тз
- Схема события описана в `schema/movie_event.proto`.
- Схема регистрируется в Schema Registry сервисом `schema-init`.
- Topic `movie-events` создаётся сервисом `kafka-init` с 3 partition.
- Producer принимает JSON, валидирует поля и enum-значения, пишет в Kafka с key=`user_id`.
- Key=`user_id` выбран, чтобы события одного пользователя попадали в одну partition и сохраняли порядок пользовательской сессии.
- Producer использует `acks=all` через `RequiredAcks: kafka.RequireAll`.
- Ошибки публикации обрабатываются retry с exponential backoff.
- ClickHouse читает topic через Kafka Engine и сохраняет raw-события в MergeTree через materialized view.
- Интеграционный тест проверяет путь `Producer -> Kafka -> ClickHouse`.
- Aggregation Service отдельным контейнером.
- Читает raw-события из ClickHouse.
- Интервал scheduler задаётся через `AGGREGATION_INTERVAL`.
- Есть ручной endpoint `/aggregate?date=YYYY-MM-DD`.
- Метрики считаются агрегатными функциями ClickHouse: `uniqExact`, `avgIf`, `countIf`.
- Результаты материализуются в ClickHouse: `day_metrics`, `top_movies_daily`, `retention_cohorts`.
- PostgreSQL хранит готовые метрики в `daily_metrics`.
- Запись в PostgreSQL идемпотентная через `ON CONFLICT(date, metric_name) DO UPDATE`.

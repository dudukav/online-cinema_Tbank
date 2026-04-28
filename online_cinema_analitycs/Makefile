.PHONY: proto-gen up down logs generate aggregate test-integration ps

up:
	docker compose up -d --build

down:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

generate:
	curl -s -X POST http://127.0.0.1:8080/events/generate

aggregate:
	curl -s "http://127.0.0.1:8082/aggregate?date=$${DATE:-$$(date +%F)}"

test-integration:
	cd producer && RUN_INTEGRATION_TESTS=1 go test ./tests -v

proto-gen:
	mkdir -p producer/proto
	protoc \
		--go_out=producer/proto \
		--go_opt=paths=source_relative \
		schema/movie_event.proto

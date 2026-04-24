.PHONY: proto-gen
proto-gen:
	mkdir -p producer/proto
	protoc \
		--go_out=producer/proto \
		--go_opt=paths=source_relative \
		schema/movie_event.proto
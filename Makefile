.PHONY: build test lint docker-up docker-down run clean

build:
	go build -o bin/polygate ./cmd/polygate

test:
	go test ./...

lint:
	golangci-lint run ./...

run: build
	./bin/polygate -config config.example.yaml

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-clean:
	docker compose down -v

clean:
	rm -rf bin/

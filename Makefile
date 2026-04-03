.PHONY: build test lint run clean \
       docker-up docker-down docker-clean \
       pg-mongo mongo-es pg-es

build:
	go build -o bin/polygate ./cmd/polygate

test:
	go test ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

# Full stack (all 5 DBs + Trino)
run: build
	./bin/polygate -config config.example.yaml

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-clean:
	docker compose down -v

# Profiles — lightweight deployments
pg-mongo:
	docker compose -f docker-compose.pg-mongo.yml up -d --build

mongo-es:
	docker compose -f docker-compose.mongo-es.yml up -d --build

pg-es:
	docker compose -f docker-compose.pg-es.yml up -d --build

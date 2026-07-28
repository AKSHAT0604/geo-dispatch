MODULE  := github.com/AKSHAT0604/geo-dispatch
BIN_DIR := bin
SERVICES := supply demand disco surge loadgen gateway

.PHONY: proto build test up down bench clean

proto:
	@which protoc >/dev/null 2>&1 || (echo "protoc not found; install https://grpc.io/docs/protoc-installation/" && exit 1)
	protoc --proto_path=api/proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		api/proto/*.proto

build:
	@mkdir -p $(BIN_DIR)
	@for svc in $(SERVICES); do \
		echo "building $$svc"; \
		go build -o $(BIN_DIR)/$$svc.exe ./cmd/$$svc || exit 1; \
	done

test:
	go test ./... -race -count=1

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

bench:
	go run ./cmd/loadgen $(ARGS)

clean:
	rm -rf $(BIN_DIR)

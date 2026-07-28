MODULE   := github.com/AKSHAT0604/geo-dispatch
BIN_DIR  := bin
LOG_DIR  := logs
SERVICES := supply demand disco surge loadgen gateway
RUN_SERVICES := disco supply demand surge gateway

.PHONY: proto build test up down bench clean run stop

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

# run starts disco first (the others dial it on startup) and stays in
# order after that; each binary's own defaults already point at the
# ports docker-compose and disco itself expose, so no env vars are
# needed for a single-node local run.
run:
	@mkdir -p $(LOG_DIR)
	@nohup $(BIN_DIR)/disco.exe > $(LOG_DIR)/disco.log 2>&1 & echo $$! > $(LOG_DIR)/disco.pid
	@sleep 1
	@for svc in supply demand surge gateway; do \
		nohup $(BIN_DIR)/$$svc.exe > $(LOG_DIR)/$$svc.log 2>&1 & echo $$! > $(LOG_DIR)/$$svc.pid; \
	done
	@echo "All services started. Map UI: http://localhost:8086"
	@echo "Logs in ./$(LOG_DIR)/, PIDs in ./$(LOG_DIR)/*.pid. Run 'make stop' to stop."

stop:
	@for svc in $(RUN_SERVICES); do \
		if [ -f $(LOG_DIR)/$$svc.pid ]; then kill $$(cat $(LOG_DIR)/$$svc.pid) 2>/dev/null || true; rm -f $(LOG_DIR)/$$svc.pid; fi; \
	done
	@echo "All services stopped."

clean:
	rm -rf $(BIN_DIR) $(LOG_DIR)

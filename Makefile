.PHONY: build run clean clean-all clean-cache test help proto

# Binary name
BINARY_NAME=key_value_store

# Build the project
build:
	go build -o $(BINARY_NAME)

# Generate protobuf code
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	    internal/raft/RPC/raft.proto

# Clean build artifacts and binary
clean:
	go clean
	rm -f $(BINARY_NAME)
	rm -rf ./data

.PHONY: build run clean clean-all clean-cache test help

# Binary name
BINARY_NAME=key_value_store

# Build the project
build:
	go build -o $(BINARY_NAME)

# Clean build artifacts and binary
clean:
	go clean
	rm -f $(BINARY_NAME)
	rm -rf ./db

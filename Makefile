.PHONY: all build test proto clean

all: build

build:
	go build -o bin/godfs-master$(EXE) ./cmd/master
	go build -o bin/godfs-chunk$(EXE) ./cmd/chunkserver
	go build -o bin/godfs-client$(EXE) ./cmd/client

test:
	go test ./...

proto:
	protoc --go_out=. --go_opt=module=godfs --go-grpc_out=. --go-grpc_opt=module=godfs -I. api/proto/godfs/v1/godfs.proto

clean:
	rm -rf bin/

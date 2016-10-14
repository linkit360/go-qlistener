.PHONY: dev run release

dev:
	go build -v -o bin/qlistener \
	./bin/qlistener

run:
	./bin/qlistener

release:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/qlistener-linux-amd64

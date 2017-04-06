.PHONY:rm run build

rm:
	rm -v bin/qlistener-linux-amd64; \
    rm -v ~/linkit/qlistener-linux-amd64;

cp:
	cp bin/qlistener-linux-amd64 ~/linkit/; cp dev/qlistener.yml ~/linkit/;

build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/qlistener-linux-amd64;

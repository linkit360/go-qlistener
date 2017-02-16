.PHONY:rm run build

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

rm:
	rm -v bin/qlistener-linux-amd64; \
    rm -v ~/linkit/qlistener-linux-amd64;

build:
	export GOOS=linux; export GOARCH=amd64; \
	sed -i "s/%VERSION%/$(VERSION)/g" /home/centos/vostrok/utils/metrics/metrics.go; \
  go build -ldflags "-s -w" -o bin/qlistener-linux-amd64; cp bin/qlistener-linux-amd64 ~/linkit/; cp dev/qlistener.yml ~/linkit/;

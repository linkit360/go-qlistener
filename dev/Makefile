.PHONY: dev run build

dev:
	rm ./qlistener; go build ;
	export GEOIP_PATH="dev/GeoLite2-City.mmdb"; ./qlistener;

run:
	export GEOIP_PATH="dev/GeoLite2-City.mmdb"; ./qlistener

build:
	rm -v bin/qlistener-linux-amd64; \
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/qlistener-linux-amd64
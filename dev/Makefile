.PHONY: dev run release config

dev:
	rm ./qlistener; go build ;
	export GEOIP_PATH="dev/GeoLite2-City.mmdb"; ./qlistener;

run:
	export GEOIP_PATH="dev/GeoLite2-City.mmdb"; ./qlistener

release:
	rm -v bin/qlistener-linux-amd64; \
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/qlistener-linux-amd64

send:
		scp -i ~/.ssh/mobilink_centos.pem bin/qlistener-linux-amd64 centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/qlistener/bin/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/qlistener/dev/

config:
	scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/qlistener/dev/


cqrcampaigns:
	curl http://localhost:50304/cqr?t=campaigns

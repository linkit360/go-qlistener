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

sendprod:
		scp -i ~/.ssh/mobilink_centos.pem bin/qlistener-linux-amd64 centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/qlistener/bin/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/qlistener/dev/

senddev:
        ssh -i ~/.ssh/mobilink_centos_testing_stage.pem centos@35.154.8.158 rm /home/centos/go/src/github.com/vostrok/qlistener/bin/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem bin/qlistener-linux-amd64 centos@35.154.8.158:/home/centos/go/src/github.com/vostrok/qlistener/bin/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/appconfig.yml centos@35.154.8.158:/home/centos/go/src/github.com/vostrok/qlistener/dev/


cqrcampaigns:
	curl http://localhost:50304/cqr?t=campaigns

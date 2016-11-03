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


senddev:
        # ssh -i ~/.ssh/mobilink_centos_testing_stage.pem centos@35.154.8.158 rm /home/centos/linkit/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem bin/qlistener-linux-amd64 centos@35.154.8.158:/home/centos/linkit/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/qlistener.yml centos@35.154.8.158:/home/centos/linkit/

config:
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/qlistener.yml centos@35.154.8.158:/home/centos/linkit/

sendprod:
        ssh -i ~/.ssh/mobilink_centos.pem  centos@52.66.23.201 rm /home/centos/linkit/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem bin/qlistener-linux-amd64 centos@52.66.23.201:/home/centos/linkit/qlistener-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/qlistener.prod.yml centos@52.66.23.201:/home/centos/linkit/


cqrcampaigns:
	curl http://localhost:50304/cqr?t=campaigns

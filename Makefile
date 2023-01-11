clean:
	rm -r bin

build: clean
	mkdir -p bin && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -o bin/main github.com/RedHatInsights/xjoin-validation

run: build
	./bin/main

test:
	ginkgo ./...
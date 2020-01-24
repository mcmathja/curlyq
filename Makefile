dependencies:
	go get github.com/markbates/pkger/cmd/pkger@v0.12.8

build:
	go build ./...

package:
	pkger

format:
	go fmt

tidy:
	go mod tidy

test:
	go test -race

release: build package format tidy test

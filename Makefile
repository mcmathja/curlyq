package:
	pkger
	go fmt

build: package
	go build ./...

test: package
	go test -race

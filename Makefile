package:
	pkger

build: package
	go build ./...

test: package
	go test -race

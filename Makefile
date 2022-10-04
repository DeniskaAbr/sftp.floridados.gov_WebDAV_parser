all: generate build
build:
	env GOOS=linux GOARCH=amd64 go build -o ./bin/cor_data_pereodic_parser cmd/main.go
	env GOOS=linux GOARCH=amd64 go build -o ./bin/cor_data_parser cmd/main.go
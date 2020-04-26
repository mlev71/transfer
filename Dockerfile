FROM golang:latest

WORKDIR /transfer
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY go.mod .

RUN go build -o tranfser cmd/transfer/main.go
EXPOSE 80

ENTRYPOINT ["./transfer"]

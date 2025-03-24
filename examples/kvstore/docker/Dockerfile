FROM golang:1.23-alpine AS builder

WORKDIR /app
RUN apk --no-cache add git
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN go build -o kvstore .

# Use a small alpine image for the final container
FROM alpine:latest

WORKDIR /app
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/kvstore /app/kvstore

RUN mkdir -p /data/raft_storage
RUN adduser -D -h /app kvstore
VOLUME /data
EXPOSE 14200 24200

ENTRYPOINT ["/app/kvstore"]
CMD ["server"]

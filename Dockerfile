FROM golang:1.22-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/rv-node ./cmd/node

FROM alpine:3.20

RUN adduser -D -H -u 10001 rv
RUN mkdir -p /data && chown -R rv:rv /data
USER rv

WORKDIR /app
COPY --from=build /out/rv-node /usr/local/bin/rv-node

EXPOSE 8080 9001
ENTRYPOINT ["/usr/local/bin/rv-node"]

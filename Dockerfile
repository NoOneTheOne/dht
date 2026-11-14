FROM node:20-alpine AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN npm install
COPY web/ .
RUN npm run build

FROM golang:1.26-alpine AS builder

ARG VERSION=dev
ARG REVISION=unknown
ARG APP_ENV=unknown

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X github.com/NoOneTheOne/dht/internal/logging.Version=${VERSION} -X github.com/NoOneTheOne/dht/internal/logging.Revision=${REVISION} -X github.com/NoOneTheOne/dht/internal/logging.Environment=${APP_ENV}" -o /build/dht-spider ./cmd/spider
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X github.com/NoOneTheOne/dht/internal/logging.Version=${VERSION} -X github.com/NoOneTheOne/dht/internal/logging.Revision=${REVISION} -X github.com/NoOneTheOne/dht/internal/logging.Environment=${APP_ENV}" -o /build/dht-getpeers ./cmd/getpeers
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X github.com/NoOneTheOne/dht/internal/logging.Version=${VERSION} -X github.com/NoOneTheOne/dht/internal/logging.Revision=${REVISION} -X github.com/NoOneTheOne/dht/internal/logging.Environment=${APP_ENV}" -o /build/dht-search ./cmd/search

FROM alpine:3.21 AS spider

ARG VERSION
ARG REVISION
LABEL org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 appuser

WORKDIR /app
COPY --from=builder /build/dht-spider .
USER appuser

EXPOSE 6881/udp

CMD ["./dht-spider"]

FROM alpine:3.21 AS getpeers

ARG VERSION
ARG REVISION
LABEL org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 appuser

WORKDIR /app
COPY --from=builder /build/dht-getpeers .
USER appuser

CMD ["./dht-getpeers"]

FROM alpine:3.21 AS search

ARG VERSION
ARG REVISION
LABEL org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$REVISION

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 appuser

WORKDIR /app
COPY --from=builder /build/dht-search .
COPY --from=web-builder /web/dist ./web
USER appuser

EXPOSE 30080

CMD ["./dht-search"]

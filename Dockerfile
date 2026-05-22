# syntax=docker/dockerfile:1.7
FROM registry.nalet.cloud/infrastructure/library/golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/download-gateway ./cmd/server

FROM registry.nalet.cloud/infrastructure/library/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/download-gateway /download-gateway
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/download-gateway"]

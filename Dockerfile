# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X main.VERSION=${VERSION}" \
    -o /out/ntfy-mcp .

# Distroless static includes CA certificates for HTTPS ntfy instances. The
# nonroot tag fixes the runtime user at 65532.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ntfy-mcp /usr/local/bin/ntfy-mcp

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ntfy-mcp"]

# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS build
WORKDIR /src

ARG CMD=relay-server
# proxy.golang.org returns 403 on some networks and Go does not fall through on 403.
ENV GOPROXY=https://goproxy.io,direct

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    case "${CMD}" in \
      relay-server) PKG=./cmd/relay-server ;; \
      example-worker) PKG=./examples/worker ;; \
      example-producer) PKG=./examples/producer ;; \
      *) echo "unknown CMD=${CMD}" >&2; exit 1 ;; \
    esac; \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app "${PKG}"

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 7420
ENTRYPOINT ["/app"]
CMD ["serve"]

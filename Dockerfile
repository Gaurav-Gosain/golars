# Multi-stage build: compile the CLI + MCP server in a full Go image,
# then copy them into a tiny distroless base. The result is a 40 MiB
# image with the two binaries on $PATH and no shell: exactly what
# you want for `docker run ghcr.io/gaurav-gosain/golars sql '...'`.

FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build with SIMD enabled on amd64: the emitted binary falls back to
# the scalar path on platforms without AVX2.
ARG GOEXPERIMENT=simd
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags "-s -w" -o /out/golars ./cmd/golars \
 && go build -trimpath -ldflags "-s -w" -o /out/golars-mcp ./cmd/golars-mcp \
 && go build -trimpath -ldflags "-s -w" -o /out/golars-lsp ./cmd/golars-lsp

# Distroless base for the runtime image. `static-debian12` has no
# glibc, no shell, no package manager: just the CA certs.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /work
COPY --from=build /out/golars /usr/local/bin/golars
COPY --from=build /out/golars-mcp /usr/local/bin/golars-mcp
COPY --from=build /out/golars-lsp /usr/local/bin/golars-lsp
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/golars"]
CMD ["help"]

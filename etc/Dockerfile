ARG OS=linux
ARG ARCH=amd64

# Creates a docker image for pgmanager

# Build stage
FROM --platform=${OS}/${ARCH} golang:1.25 AS builder
WORKDIR /usr/src/app
COPY . .
RUN go install github.com/djthorpe/go-wasmbuild/cmd/wasmbuild@latest && \
    OS=${OS} ARCH=${ARCH} make cmd/pgmanager

# Runtime stage
FROM --platform=${OS}/${ARCH} debian:trixie-slim
ARG SOURCE=https://github.com/mutablelogic/go-pg
COPY --from=builder /usr/src/app/build/ /usr/local/bin/
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Labels
LABEL org.opencontainers.image.source=${SOURCE}

# Entrypoint when running the server
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/pgmanager"]

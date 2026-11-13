# syntax=docker/dockerfile:1

# pgmanager, beside PostgreSQL, in one container, under s6-overlay, on top
# of docker-postgres's image (its primary and replica set-up, SSL, PostGIS
# and pgvector). PostgreSQL's version is PG_VERSION, one of docker-postgres's
# tags, such as 17-bookworm or 18-trixie.
ARG PG_VERSION=17-bookworm
ARG S6_OVERLAY_VERSION=3.2.3.2

# The web UI, which installs @mutablelogic/go-server from GitHub Packages,
# with a token in the github_token secret
FROM node:24-bookworm AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json frontend/.npmrc ./
RUN --mount=type=secret,id=github_token,env=GITHUB_TOKEN npm ci --silent
COPY frontend/ ./
RUN npm run --silent check && npm run --silent build

# pgmanager, with the web UI embedded
FROM golang:1.27-bookworm AS builder
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend /src/frontend/dist ./frontend/dist
RUN CGO_ENABLED=0 go build -tags frontend \
    -ldflags "-s -w -X github.com/mutablelogic/go-server/pkg/version.GitTag=v${VERSION} -X github.com/mutablelogic/go-server/pkg/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /out/pgmanager ./cmd/pgmanager

# PostgreSQL, from docker-postgres, with s6-overlay, and pgmanager
FROM ghcr.io/mutablelogic/docker-postgres:${PG_VERSION}
ARG PG_VERSION
ARG S6_OVERLAY_VERSION
ARG TARGETARCH
ARG VERSION=dev
ARG SOURCE=https://github.com/mutablelogic/go-pg
LABEL org.opencontainers.image.source=${SOURCE} \
    org.opencontainers.image.description="pgmanager beside PostgreSQL ${PG_VERSION}, under s6-overlay" \
    org.opencontainers.image.version="${VERSION}-${PG_VERSION}"

# s6-overlay, for the architecture
RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils && rm -rf /var/lib/apt/lists/* && \
    case "${TARGETARCH}" in amd64) S6_ARCH=x86_64 ;; arm64) S6_ARCH=aarch64 ;; *) echo "unsupported architecture ${TARGETARCH}" && exit 1 ;; esac && \
    for f in s6-overlay-noarch s6-overlay-${S6_ARCH}; do \
        curl -fsSL "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/${f}.tar.xz" | tar -C / -Jxp; \
    done

# pgmanager, its restart command, and the services
COPY --from=builder /out/pgmanager /usr/local/bin/pgmanager
COPY --chmod=755 etc/bin/pg-restart /usr/local/bin/pg-restart
COPY --chmod=755 etc/s6-overlay/ /etc/s6-overlay/

# The services have the container's environment, such as POSTGRES_PASSWORD,
# PostgreSQL's initialisation isn't timed out, and pgmanager's settings
ENV S6_KEEP_ENV=1 \
    S6_CMD_WAIT_FOR_SERVICES_MAXTIME=0 \
    S6_BEHAVIOUR_IF_STAGE2_FAILS=2 \
    PGMANAGER_ENABLED=true \
    PGMANAGER_ADDR=:8080

# s6-overlay stops on SIGTERM, which stops pgmanager, then PostgreSQL, with a
# fast shutdown, rather than PostgreSQL's own SIGINT, which the image has
EXPOSE 5432 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/init"]
CMD []

# Path parameters
GO ?= $(shell which go  2>/dev/null)
NPM ?= $(shell which npm 2>/dev/null)
DOCKER ?= $(shell which docker 2>/dev/null)
WASMBUILD ?= $(shell which wasmbuild 2>/dev/null)

# Locations
BUILD_DIR ?= build
CMD_DIR := $(filter-out cmd/_%,$(wildcard cmd/*))
TS_DIR := frontend

# The commands embed the web UI, with the "frontend" tag, once it's built
# into frontend/dist, by make frontend, which needs npm, and a token which
# can read GitHub Packages, in GITHUB_TOKEN
BUILD_TAGS = $(if $(wildcard $(TS_DIR)/dist/index.html),-tags frontend,)

# Set OS and Architecture
ARCH ?= $(shell arch | tr A-Z a-z | sed 's/x86_64/amd64/' | sed 's/i386/amd64/' | sed 's/armv7l/arm/' | sed 's/aarch64/arm64/')
OS ?= $(shell uname | tr A-Z a-z)
VERSION ?= $(shell git describe --tags --always | sed 's/^v//')

# Set build flags
VERSION_PKG = github.com/mutablelogic/go-server/pkg/version
BUILD_LD_FLAGS += -X $(VERSION_PKG).GitTag=$(shell git describe --tags --always)
BUILD_LD_FLAGS += -X $(VERSION_PKG).GitBranch=$(shell git name-rev HEAD --name-only --always)
BUILD_LD_FLAGS += -X $(VERSION_PKG).BuildDate=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)
BUILD_FLAGS = -ldflags "-s -w ${BUILD_LD_FLAGS}" 

# Docker
DOCKER_REPO ?= ghcr.io/mutablelogic/pgmanager
DOCKER_SOURCE ?= $(shell cat go.mod | head -1 | cut -d ' ' -f 2)
DOCKER_TAG = ${DOCKER_REPO}:${VERSION}-${OS}-${ARCH}

###############################################################################
# ALL

.PHONY: all
all: build

###############################################################################
# BUILD

# Build the web UI, and then the commands in the cmd directory, which embed
# it
.PHONY: build
build: frontend
	@$(MAKE) --no-print-directory tidy $(CMD_DIR)

$(CMD_DIR): go-dep mkdir
	@echo Build command $(notdir $@) GOOS=${OS} GOARCH=${ARCH} $(BUILD_TAGS)
	@GOOS=${OS} GOARCH=${ARCH} ${GO} build $(BUILD_TAGS) ${BUILD_FLAGS} -o ${BUILD_DIR}/$(notdir $@) ./$@

# Build the web UI into frontend/dist, from package-lock.json, once it's
# type checked, as esbuild doesn't check its types
.PHONY: frontend
frontend: npm-dep mkdir
	@echo Build frontend
	@cd $(TS_DIR) && ${NPM} ci --silent && ${NPM} run --silent check && ${NPM} run --silent build

# Test the web UI
.PHONY: frontend-test
frontend-test: npm-dep
	@echo Frontend Tests
	@cd $(TS_DIR) && ${NPM} ci --silent && ${NPM} run --silent check && ${NPM} test --silent

# Build the docker image
.PHONY: docker
docker: docker-dep ${NPM_DIR}
	@echo build docker image ${DOCKER_TAG} OS=${OS} ARCH=${ARCH} SOURCE=${DOCKER_SOURCE} VERSION=${VERSION}
	@${DOCKER} build \
		--tag ${DOCKER_TAG} \
		--provenance=false \
		--build-arg ARCH=${ARCH} \
		--build-arg OS=${OS} \
		--build-arg SOURCE=${DOCKER_SOURCE} \
		--build-arg VERSION=${VERSION} \
		-f etc/Dockerfile .

# Push docker container
.PHONY: docker-push
docker-push: docker-dep 
	@echo push docker image: ${DOCKER_TAG}
	@${DOCKER} push ${DOCKER_TAG}

# Print out the version
.PHONY: docker-version
docker-version: docker-dep 
	@echo "tag=${VERSION}"

###############################################################################
# TEST

.PHONY: test
test: unit-test coverage-test

.PHONY: unit-test
unit-test: go-dep
	@echo Unit Tests
	@${GO} test .
	@${GO} test ./pgmanager/...
	@${GO} test ./pgqueue/...
	@${GO} test ./pkg/...

.PHONY: coverage-test
coverage-test: go-dep mkdir
	@echo Test Coverage
	@${GO} test -v -coverprofile ${BUILD_DIR}/coverprofile.out ./pkg/... ./pgqueue/... ./pgmanager/...
	@${GO} tool cover -func ${BUILD_DIR}/coverprofile.out > ${BUILD_DIR}/coverage.txt

###############################################################################
# CLEAN

# Other rules
.PHONY: mkdir
mkdir:
	@install -d $(BUILD_DIR)

.PHONY: go-dep tidy
tidy:
	@echo 'go tidy'
	@$(GO) mod tidy

.PHONY: clean
clean: tidy
	@echo 'clean'
	@rm -fr $(BUILD_DIR) $(TS_DIR)/dist
	@$(GO) clean

###############################################################################
# DEPENDENCIES

.PHONY: go-dep
go-dep:
	@test -f "${GO}" && test -x "${GO}"  || (echo "Missing go binary" && exit 1)

.PHONY: npm-dep
npm-dep:
	@test -f "${NPM}" && test -x "${NPM}"  || (echo "Missing npm binary, which builds the web UI" && exit 1)

.PHONY: docker-dep
docker-dep:
	@test -f "${DOCKER}" && test -x "${DOCKER}"  || (echo "Missing docker binary" && exit 1)

.PHONY: wasmbuild-dep
wasmbuild-dep:
	@test -f "${WASMBUILD}" && test -x "${WASMBUILD}"  || (echo "Missing wasmbuild binary" && exit 1)
#!/usr/bin/env bash

set -Eeuo pipefail

readonly BASE_IMAGE="justflow:local"
readonly REFERENCE_IMAGE="justflow-host-application:local"
readonly COMPOSE_WAIT_TIMEOUT_SECONDS=120

SCRIPT_DIRECTORY="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIRECTORY
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIRECTORY}/.." && pwd)"
readonly REPOSITORY_ROOT

log() {
    printf '%s\n' "$*"
}

fail() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    local command_name="$1"

    command -v "${command_name}" >/dev/null 2>&1 \
        || fail "required command is not available: ${command_name}"
}

main() {
    require_command docker
    require_command git

    docker compose version >/dev/null 2>&1 \
        || fail "Docker Compose is not available through 'docker compose'"
    docker info >/dev/null 2>&1 \
        || fail "Docker is not running or is not accessible"
    git -C "${REPOSITORY_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
        || fail "the repository checkout is required to derive image metadata"

    local source_revision
    local build_date
    local source_url
    source_revision="$(git -C "${REPOSITORY_ROOT}" rev-parse HEAD)"
    build_date="$(git -C "${REPOSITORY_ROOT}" show -s --format=%cI HEAD)"
    source_url="$(git -C "${REPOSITORY_ROOT}" remote get-url origin)"

    cd "${REPOSITORY_ROOT}"

    log "Building ${BASE_IMAGE}"
    docker build \
        --file docker/Dockerfile \
        --tag "${BASE_IMAGE}" \
        --build-arg "SOURCE_REVISION=${source_revision}" \
        --build-arg "BUILD_DATE=${build_date}" \
        --build-arg "SOURCE_URL=${source_url}" \
        .

    log "Building ${REFERENCE_IMAGE}"
    docker build \
        --file examples/host_application/Dockerfile \
        --tag "${REFERENCE_IMAGE}" \
        --build-arg "JUSTFLOW_BASE_IMAGE=${BASE_IMAGE}" \
        --build-arg "SOURCE_REVISION=${source_revision}" \
        --build-arg "BUILD_DATE=${build_date}" \
        --build-arg "SOURCE_URL=${source_url}" \
        .

    log "Validating compose.yaml"
    docker compose config --quiet

    log "Starting the local stack"
    docker compose up \
        --detach \
        --wait \
        --wait-timeout "${COMPOSE_WAIT_TIMEOUT_SECONDS}"

    docker compose ps --all

    log "Host application API: http://127.0.0.1:8080"
    log "Temporal UI: http://127.0.0.1:8233"
    log "Administration UI: not exposed by this Compose application"
    log "Follow logs: docker compose logs --follow api worker"
    log "Stop: docker compose down"
}

main "$@"

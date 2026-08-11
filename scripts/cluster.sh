#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)

if ! command -v docker >/dev/null 2>&1; then
  echo "error: Docker is required" >&2
  exit 127
fi

compose=(docker compose --project-directory "${PROJECT_DIR}" --file "${PROJECT_DIR}/docker-compose.yml")

if [[ -n "${RAFTKV_ENV_FILE:-}" ]]; then
  env_file=${RAFTKV_ENV_FILE}
  if [[ ${env_file} != /* ]]; then
    env_file="${PROJECT_DIR}/${env_file}"
  fi
  if [[ ! -f ${env_file} ]]; then
    echo "error: environment file not found: ${env_file}" >&2
    exit 2
  fi
  compose+=(--env-file "${env_file}")
fi

usage() {
  cat <<'USAGE'
Usage: scripts/cluster.sh <command>

Commands:
  up        Build the image and start all three nodes
  start     Start an existing stopped cluster
  stop      Stop the cluster without removing containers or volumes
  restart   Restart all nodes
  status    Show container status
  logs      Follow logs from all nodes (Ctrl-C stops following)
  down      Remove containers and the network; preserve data volumes

Set RAFTKV_ENV_FILE to use a Compose environment file. This script never
deletes the named data volumes.
USAGE
}

case "${1:-}" in
  up)
    "${compose[@]}" up --detach --build
    "${compose[@]}" ps
    echo "Cluster is starting. Published RESP endpoints:"
    smoke_ports=""
    for service in raftkv-1 raftkv-2 raftkv-3; do
      published=$("${compose[@]}" port "${service}" 6379)
      echo "  ${service}: ${published}"
      published_port=${published##*:}
      smoke_ports+="${smoke_ports:+,}${published_port}"
    done
    echo "Run RAFTKV_PORTS=${smoke_ports} scripts/smoke.sh to wait for a leader and verify SET/GET."
    ;;
  start)
    "${compose[@]}" start
    ;;
  stop)
    "${compose[@]}" stop
    ;;
  restart)
    "${compose[@]}" restart
    ;;
  status)
    "${compose[@]}" ps
    ;;
  logs)
    "${compose[@]}" logs --follow --tail=200
    ;;
  down)
    "${compose[@]}" down
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

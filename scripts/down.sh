#!/usr/bin/env bash
# Arrete l'appli (via son pidfile) puis le cluster docker compose. Les
# donnees Kafka sont perdues de toute facon (pas de volume nomme dans
# docker-compose.yml) ; l'etat RocksDB local de l'appli, lui, survit
# a ce script (cf. reset.sh pour un vrai nettoyage).
#
# Usage : scripts/down.sh [--volumes]
#   --volumes   passe -v a `docker compose down` (pas de volume nomme
#               aujourd'hui, mais evite un oubli si on en ajoute un).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh

VOLUMES_FLAG=""
[ "${1:-}" = "--volumes" ] && VOLUMES_FLAG="-v"

if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE")"
    if kill -0 "$PID" 2>/dev/null; then
        echo "[down] arret de l'appli (GROUPE=$GROUPE, pid $PID)..."
        kill "$PID"
        timeout 30 bash -c "while kill -0 $PID 2>/dev/null; do sleep 1; done" || {
            echo "[down] pid $PID recalcitrant, kill -9" >&2
            kill -9 "$PID" 2>/dev/null || true
        }
    fi
    rm -f "$PID_FILE"
else
    echo "[down] pas d'appli en cours pour GROUPE=$GROUPE."
fi

echo "[down] arret du cluster docker compose..."
# shellcheck disable=SC2086
docker compose down $VOLUMES_FLAG

echo "[down] termine."

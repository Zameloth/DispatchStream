#!/usr/bin/env bash
# Variables partagees par up.sh / down.sh / reset.sh. A sourcer, pas a executer.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# .env (ignore par git, cf. .env.example) prime sur les
# variables deja dans l'environnement, qui priment elles-memes sur les
# defauts ci-dessous.
ENV_FILE="${ROOT_DIR}/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

export GROUPE="${GROUPE:-dev}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

APP_ID="dispatch-${GROUPE}"
# Doit rester aligne avec le state.dir par defaut de Kafka Streams
# (java.io.tmpdir/kafka-streams/<application.id>) : c'est cet etat RocksDB
# local, jamais touche par docker compose down, qui doit etre efface pour
# repartir d'un run vraiment propre (cf. reset.sh).
STATE_DIR="/tmp/kafka-streams/${APP_ID}"

RUN_DIR="${ROOT_DIR}/scripts/.run"
PID_FILE="${RUN_DIR}/app-${GROUPE}.pid"
LOG_FILE="${RUN_DIR}/app-${GROUPE}.log"

INPUT_TOPICS=(dispatch.ride.requests dispatch.driver.locations dispatch.trip.events)

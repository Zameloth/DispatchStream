#!/usr/bin/env bash
# Demarre le cluster Kafka local (docker compose), cree les topics d'entree,
# et lance l'appli packagee en arriere-plan (GROUPE, KAFKA_BOOTSTRAP depuis
# .env ou l'environnement, cf. env.sh). Idempotent : si l'appli
# tourne deja pour ce GROUPE, ne relance rien.
#
# Usage : scripts/up.sh [--build]
#   --build   force un `mvn package` meme si le jar existe deja.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh

FORCE_BUILD=0
[ "${1:-}" = "--build" ] && FORCE_BUILD=1

echo "[up] cluster docker compose..."
docker compose up -d

echo "[up] attente du broker kafka-1..."
timeout 60 bash -c 'until docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done'
echo "[up] kafka pret."

for topic in "${INPUT_TOPICS[@]}"; do
    docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 >/dev/null
done
echo "[up] topics d'entree prets : ${INPUT_TOPICS[*]}"

JAR="target/quarkus-app/quarkus-run.jar"
if [ "$FORCE_BUILD" = "1" ] || [ ! -f "$JAR" ]; then
    echo "[up] build (mvn package)..."
    mvn -q -DskipTests package
fi

mkdir -p "$RUN_DIR"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "[up] appli deja lancee pour GROUPE=$GROUPE (pid $(cat "$PID_FILE")), rien a faire."
    exit 0
fi

echo "[up] lancement de l'appli (GROUPE=$GROUPE, KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP)..."
GROUPE="$GROUPE" KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" java -jar "$JAR" > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

if timeout 60 bash -c "until grep -q 'State transition from REBALANCING to RUNNING' '$LOG_FILE' 2>/dev/null; do sleep 2; done"; then
    echo "[up] appli prete (pid $(cat "$PID_FILE"), logs: $LOG_FILE)"
else
    echo "[up] l'appli n'a pas atteint l'etat RUNNING sous 60s, voir $LOG_FILE" >&2
    exit 1
fi

echo "[up] Kafbat UI : http://localhost:8080"
echo "[up] HTTP appli : http://localhost:8090"
echo "[up] DLQ topic  : ${GROUPE}.dispatch.dlq"

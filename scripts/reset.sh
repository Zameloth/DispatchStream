#!/usr/bin/env bash
# Nettoyage complet + redemarrage a blanc : down.sh (avec volumes) + purge
# de l'etat RocksDB local de l'appli (/tmp/kafka-streams/dispatch-<GROUPE>,
# qui ne disparait PAS avec docker compose down) + up.sh.
#
# A utiliser pour rejouer data/raw et obtenir des stats representatives
# (sans etat residuel d'un run precedent qui fausserait le taux de
# duplicate/late).
#
# Usage : scripts/reset.sh [--build]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh

./scripts/down.sh --volumes

echo "[reset] purge de l'etat local RocksDB ($STATE_DIR)..."
rm -rf "$STATE_DIR"

./scripts/up.sh "${1:-}"

echo "[reset] cluster + appli repartis a blanc pour GROUPE=$GROUPE."
echo "[reset] pour rejouer le dataset : python scripts/rejouer.py --dossier data/raw --bootstrap $KAFKA_BOOTSTRAP"

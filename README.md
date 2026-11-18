# DISPATCH — pipeline VTC temps réel (Quarkus + Kafka Streams)

Pipeline de streaming pour une plateforme de VTC fictive opérant sur Paris.
L'application consomme trois flux Kafka (demandes de course, pings GPS des
chauffeurs, événements de course) volontairement sales et :

- **valide** chaque message et envoie les invalides dans une DLQ, avec le message original et une raison précise, **sans jamais crasher** ;
- calcule la **demande** par zone (fenêtres tumbling de 2 min) ;
- calcule l'**offre** par zone, à partir de la dernière position connue de chaque chauffeur ;
- déclenche le **surge pricing** quand `demande / max(offre, 1) > 2` ;
- signale les **courses fantômes** (vitesse moyenne > 130 km/h ou durée > 3 h) ;
- expose le surge courant par zone via une **API REST Interactive Queries**.

> Projet réalisé en binôme dans le cadre d'un module Kafka à l'ESGI (2026).
> Le sujet imposait Quarkus 3.19 + Kafka Streams et les topics d'entrée.

## Architecture

```
dispatch.ride.requests ────┐
dispatch.driver.locations ─┼─> [ validation stateless ] ─> [ validation stateful ] ──> invalides ──> <grp>.dispatch.dlq
dispatch.trip.events ──────┘   champs, types, bornes GPS,   retard, doublons,
                               enums, timestamp              conflits, END orphelins
                                                    │
                                                    ▼ valides
              ┌───────────────┬─────────────────────┴───────┬──────────────────┐
              ▼               ▼                             ▼                  ▼
        demande/zone    offre/zone                   surge (ratio)      courses fantômes
        tumbling 2 min  KTable par chauffeur         jointure           vitesse > 130 km/h
        grace 6 min     → ré-agrégation par zone     table-table        ou durée > 3 h
              │               │                             │                  │
              ▼               ▼                             ▼                  ▼
        <grp>.dispatch. <grp>.dispatch.              <grp>.dispatch.    <grp>.dispatch.
        demand.by-zone  supply.by-zone               surge              alerts.trips
                                                            │
                                                            ▼
                                          state store → GET /api/v1/surge/{zone}
```

Les 12 zones : `Z-CHATELET, Z-MONTMARTRE, Z-BASTILLE, Z-BERCY, Z-DEFENSE,
Z-ETOILE, Z-MONTPARNASSE, Z-BELLEVILLE, Z-ODEON, Z-PIGALLE, Z-NATION, Z-AUTEUIL`.

Le vocabulaire métier (événement invalide, en retard, doublon, conflit, END
orphelin, offre, demande, surge…) est défini dans [`CONTEXT.md`](CONTEXT.md).
Les choix de conception sont documentés dans [`docs/adr/`](docs/adr) :

| ADR | Décision |
|-----|----------|
| [0001](docs/adr/0001-continuous-emission-for-demand-by-zone.md) | Émission continue de la demande (pas de `suppress()`) pour un surge réactif |
| [0002](docs/adr/0002-dual-materialization-for-surge-ratio.md) | Ratio de surge matérialisé pour toutes les zones, filtre `> 2` uniquement sur le topic |
| [0003](docs/adr/0003-stateless-scope-for-disp-1.md) | Ingestion d'abord stateless, détections stateful dans un second temps |
| [0004](docs/adr/0004-two-phase-parse-for-malformed-vs-wrong-type.md) | Parse en deux temps pour distinguer JSON cassé et mauvais type |
| [0005](docs/adr/0005-crash-safety-net-for-disp-1.md) | `StreamsUncaughtExceptionHandler` en `REPLACE_THREAD` comme filet de sécurité |
| [0006](docs/adr/0006-table-table-join-for-surge-ratio.md) | Jointure table-table demande × offre |
| [0007](docs/adr/0007-stateful-validation-for-late-duplicate-conflicting-orphan.md) | Validation stateful (Processor API) pour les retards, doublons, conflits et END orphelins |

## Topics

### Entrées

| Topic | Clé | Contenu |
|-------|-----|---------|
| `dispatch.ride.requests` | `user_id` | `request_id`, `user_id`, `pickup{lat,lon,zone}`, `dropoff{lat,lon,zone}`, `timestamp` |
| `dispatch.driver.locations` | `driver_id` | `driver_id`, `lat`, `lon`, `zone`, `status` (`AVAILABLE`/`BUSY`/`OFFLINE`), `timestamp` |
| `dispatch.trip.events` | `trip_id` | `trip_id`, `driver_id`, `user_id`, `event` (`START`/`END`), `distance_km` et `duration_seconds` (sur les `END`), `timestamp` |

Environ 7 % des messages sont invalides : champs absents ou `null`, mauvais
types, GPS hors de Paris (`48.7 ≤ lat ≤ 49.0`, `2.2 ≤ lon ≤ 2.5`), enums
inconnues, timestamps illisibles, JSON tronqué. S'y ajoutent des retards et
des doublons.

### Sorties

Toutes les sorties sont préfixées par `GROUPE` (variable d'environnement,
`grp00` par défaut).

**`<grp>.dispatch.dlq`** : un message par rejet, avec le message original
conservé octet pour octet.

```json
{
  "source_topic": "dispatch.driver.locations",
  "key": "drv-034",
  "raw_value": "{\"driver_id\": \"drv",
  "reason": "malformed_json",
  "rejected_at": "2026-07-15T22:53:48.113Z"
}
```

Les raisons possibles :

- `malformed_json`, `wrong_type:<champ>`, `missing_field:<champ>` ;
- `gps_out_of_bounds[:pickup|:dropoff]`, `unknown_zone[:pickup|:dropoff]`, `unknown_status`, `unknown_event` ;
- `unparseable_timestamp`, `invalid_distance_km`, `invalid_duration_seconds` ;
- `late` (arrivé plus de 6 min après son timestamp) ;
- `duplicate` ;
- `conflicting location for driver at timestamp` ;
- `no matching START for trip_id`.

**`<grp>.dispatch.demand.by-zone`** (clé = zone) : émis à chaque mise à jour,
donc plusieurs fois par fenêtre (cf. ADR 0001).

```json
{ "zone": "Z-BERCY", "window_start": "2026-07-15T22:52:00Z", "window_end": "2026-07-15T22:54:00Z", "count": 17 }
```

**`<grp>.dispatch.supply.by-zone`** (clé = zone)

```json
{ "zone": "Z-BERCY", "available_drivers": 4 }
```

**`<grp>.dispatch.surge`** (clé = zone) : uniquement quand le ratio dépasse 2.

```json
{ "zone": "Z-BERCY", "ratio": 4.25 }
```

**`<grp>.dispatch.alerts.trips`** (clé = `trip_id`) : `reason` vaut
`excessive_speed` ou `excessive_duration`.

```json
{
  "trip_id": "trip-74daaebf",
  "driver_id": "drv-015",
  "reason": "excessive_speed",
  "average_speed_kmh": 2700.0,
  "distance_km": 900.0,
  "duration_seconds": 1200,
  "timestamp": "2026-07-15T22:55:41.203Z"
}
```

### API REST

`GET http://localhost:8090/api/v1/surge/{zone}` lit le state store du surge et
renvoie le ratio courant, même hors surge :

```json
{ "zone": "Z-BERCY", "ratio": 4.25, "surging": true }
```

Elle renvoie `404` pour une zone inconnue et `503` tant que le store n'est pas
encore interrogeable (démarrage ou rebalance).

## Lancer le projet

Prérequis : JDK 21, Maven 3.9+, Docker, Python 3 (pour le rejeu).

```bash
cp .env.example .env               # GROUPE, KAFKA_BOOTSTRAP
scripts/up.sh                      # cluster Kafka 3 brokers + Kafbat UI (:8080), topics d'entrée, build et lancement de l'appli (:8090)

python -m venv scripts/.venv && scripts/.venv/bin/pip install -r scripts/requirements.txt
scripts/.venv/bin/python scripts/rejouer.py --dossier data/raw --bootstrap localhost:29092
# rejoue ~192k messages réels (data/raw/*.jsonl) dans l'ordre chronologique ; --speed 1 pour le temps réel

scripts/down.sh                    # arrête l'appli et le cluster
scripts/reset.sh                   # repart à blanc (purge aussi l'état RocksDB local)
```

Les logs de l'appli sont écrits dans `scripts/.run/app-<GROUPE>.log`. Les
topics sont consultables dans Kafbat UI sur http://localhost:8080.

> `mvn quarkus:dev` ne fonctionne pas tel quel : les Dev Services de Quarkus
> tentent de lancer leur propre Kafka. Il faut lancer le jar packagé, ce que
> fait `up.sh`.

## Tests

```bash
mvn test
```

La topologie est testée avec `TopologyTestDriver` : validation stateless et
stateful pour chaque flux, demande, offre, surge, courses fantômes et API REST.
Aucun broker n'est nécessaire.

## Stack

Java 21 · Quarkus 3.19 · Kafka Streams (Kafka 4.x) · Jackson · JUnit 5 · Docker
Compose (Kafka KRaft 3 brokers + Kafbat UI).

# kbeauty-customer-mdm

Microservice Go — golden record client (MDM, Master Data Management) pour
l'ecosysteme [kbeauty-shop](https://github.com/NF31/kbeauty-shop).

Exercice de montee en competence (preparation Master MIAGE), voir le dossier
`docs/montee-competence` du repo principal pour le cadrage complet.

## Principe

`kbeauty-shop` reste proprietaire de l'identite client (table `users`), mais
`sav-ticketing-php` cree sa **propre copie** du client au moment d'importer
un retour, sans jamais la mettre a jour - si un client change de nom/email
dans la boutique, le SAV garde une version perimee indefiniment. C'est le
risque classique de divergence qu'un MDM existe pour fermer.

```
kbeauty-shop            RabbitMQ              kbeauty-customer-mdm           RabbitMQ            sav-ticketing-php
────────────            ────────              ─────────────────────         ────────            ──────────────────
profil client modifie
       │
       ▼ publie customer.updated ──────────────────► consomme, met a jour
                                                       le golden record
                                                       (ignore si message
                                                       en retard)
                                                              │
                                                              ▼ republie
                                              customer.golden_record.updated ──────► met a jour
                                                                                      sa copie du
                                                                                      client
```

Aucun acces direct a la base de donnees ou au code de `kbeauty-shop` ou de
`sav-ticketing-php` - uniquement les messages RabbitMQ echanges, plus une
petite API de lecture synchrone (`GET /customers/{email}`).

Aujourd'hui, un seul producteur (`kbeauty-shop`) alimente le golden record :
c'est donc un miroir, pas encore une vraie fusion multi-sources. L'interet
du MDM ici est l'**architecture** (upsert idempotent base sur un horodatage,
republication fiable) plutot que la complexite de fusion - voir Limites et
suite ci-dessous.

## Stack

- Go 1.27 (stdlib `net/http`, `database/sql`)
- `github.com/rabbitmq/amqp091-go` pour RabbitMQ (pas de lib de reconnexion
  automatique - meme choix que le consumer PHP du SAV : un superviseur
  externe redemarre le process en cas de coupure)
- `github.com/santhosh-tekuri/jsonschema/v5` pour valider les messages
  contre leur contrat (draft 2020-12, `AssertFormat: true`)
- `modernc.org/sqlite` (driver SQLite pur Go, pas de CGO) pour le golden
  record

## Pourquoi ce compte RabbitMQ est different des autres

Contrairement a `sav-ticketing-php` et `kbeauty-notifications-service` (qui
ne font que *consommer* `kbeauty.exchange`), ce service doit aussi y
**publier** (`customer.golden_record.updated`) : son compte RabbitMQ (`mdm`,
cree par `kbeauty-ecosystem/rabbitmq/creer-comptes.sh`) a donc un profil de
droits different - lecture ET ecriture sur l'exchange partage, toujours
sans pouvoir le supprimer ni lire les files des autres applications.

## Commandes

```bash
go build ./...
go vet ./...
gofmt -l .      # formatage (doit etre vide)
go test ./...
```

## Lancer en local

```bash
# 1. RabbitMQ + compte dedie (rejouable)
cd ~/projects/kbeauty-ecosystem
docker start kbeauty_rabbitmq
./rabbitmq/creer-comptes.sh

# 2. Ce service
cd ~/projects/kbeauty-customer-mdm
cp .env.example .env
export $(cat .env | xargs)
go run .
```

Simuler un evenement en attendant que `kbeauty-shop` le publie reellement :

```bash
docker exec kbeauty_rabbitmq rabbitmqadmin -q publish \
  exchange=kbeauty.exchange \
  routing_key=customer.updated \
  payload='{"eventType":"customer.updated","sourceUserId":1,"name":"Jeanne Martin","email":"jeanne@example.com","occurredAt":"2026-10-14T16:45:00Z"}'
```

Puis `curl http://localhost:3001/customers/jeanne@example.com`.

## Ce qui a ete verifie en local

Avec un vrai RabbitMQ (`kbeauty_rabbitmq`) et le compte `mdm` cree par
`creer-comptes.sh` :

| Scenario | Resultat |
|---|---|
| Message conforme publie | Golden record cree, `customer.golden_record.updated` republie |
| Message plus ancien que le golden record existant | Ignore, pas de regression (verifie sur le nom/email restes inchanges) |
| Message hors contrat (`sourceUserId` texte, email invalide) | Rejete vers `mdm.rejected`, jamais traite |
| `GET /customers/{email}` (existant / inconnu) | 200 avec le golden record / 404 |
| `GET /health` | `{"status":"ok","rabbitmq":"connected"}` |

Tests automatises : `go test ./...` (8 tests, store + contrat).

## Limites et suite

- **Un seul producteur pour l'instant** : rien ne publie encore
  `customer.updated` cote `kbeauty-shop`, et `sav-ticketing-php` ne consomme
  pas encore `customer.golden_record.updated` (continue de ne jamais mettre
  a jour sa copie du client). Les deux cotes restent a faire.
- **Pas de vraie fusion multi-sources** : avec un seul producteur, l'upsert
  idempotent base sur l'horodatage suffit. Un deuxieme producteur
  demanderait une vraie logique de resolution de conflit (quelle source
  gagne, sur quel champ).
- **Pas de reconnexion automatique RabbitMQ** : en cas de coupure, le
  process s'arrete et doit etre redemarre par un superviseur externe -
  choix deliberement simple, comme pour le consumer PHP du SAV.
- **Production** : pas de RabbitMQ heberge aujourd'hui, ce service ne
  tourne qu'en local pour l'instant.

## Développement assisté par IA

Ce projet est développé avec un assistant IA (Claude Code). Je définis le périmètre,
l'architecture et les priorités, puis je relis et teste chaque changement avant de le
fusionner. Les commits co-écrits avec l'IA le signalent par une ligne `Co-Authored-By`.

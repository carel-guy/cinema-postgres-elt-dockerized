# Pipeline ELT PostgreSQL pour Films

## Présentation
Ce dépôt illustre un workflow **ELT** simple mais réaliste : les données brutes sont chargées dans un PostgreSQL source, répliquées telles quelles vers un PostgreSQL de destination, puis modélisées avec dbt pour produire des vues analytiques (ex. `film_ratings`). L'objectif est de fournir une base claire que l'on peut publier sur GitHub ou étendre dans un contexte professionnel.

## Architecture
- PostgreSQL `source_postgres` : initialisé par `source_db_init/init.sql` avec des films et acteurs.
- Conteneur `elt_replicator` : exécute `elt/elt_runner.py` (pg_dump + psql) pour copier les données = « load » dans ELT.
- PostgreSQL `destination_postgres` : reçoit les tables brutes et sert de base à dbt.
- Conteneur `dbt` : lance `dbt run` dans `postgres_elt_dbt` afin de créer les modèles matérialisés.

## Prérequis
- Docker + Docker Compose.
- Python non requis (tout tourne dans les conteneurs).
- Un profil dbt nommé `postgres_elt_dbt` dans `~/.dbt/profiles.yml` lorsque vous lancez dbt hors Docker.

### Exemple de profil dbt
```yaml
postgres_elt_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 127.0.0.1
      port: 5434
      user: root
      password: 1724
      dbname: destination_db
      schema: public
      threads: 4
```

## Démarrage rapide
1. Cloner ce dépôt puis se placer à sa racine.
2. Lancer l'ensemble des services :
   ```bash
   docker compose up --build
   ```
3. Une fois `dbt` terminé, les tables/vues sont créées dans `destination_postgres`. Vous pouvez vous connecter en local (`localhost:5434`, user `root`, mot de passe `1724`).
4. (Optionnel) Utiliser `docker compose run --rm dbt dbt test` pour exécuter les tests de qualité.

## Structure du dépôt
```
├── docker-compose.yml        # Orchestration des 4 services
├── elt/                      # Code Python pour la phase Extract + Load
│   ├── Dockerfile
│   └── elt_runner.py
├── postgres_elt_dbt/         # Projet dbt (models, sources, tests)
├── source_db_init/init.sql   # Données seedées sur le Postgres source
├── Dockerfile.dbt            # Image personnalisée pour dbt
└── README.md
```

## Flux de données
1. `source_postgres` démarre et applique `init.sql`.
2. `elt_runner.py` attend que les deux bases soient disponibles, lance `pg_dump` puis `psql` pour copier toutes les tables.
3. Le conteneur `dbt` déclenche `dbt run` et construit les modèles, notamment `film_ratings` qui combine films, acteurs et métriques de qualité.
4. Les tests définis dans `postgres_elt_dbt/models/example/schema.yml` peuvent être exécutés pour valider la fraîcheur et la cohérence des données.

## Personnalisation rapide
- Les variables d'environnement `SOURCE_*` / `DESTINATION_*` définies dans `docker-compose.yml` peuvent être ajustées pour cibler d'autres bases Postgres.
- `ELT_MAX_RETRIES`, `ELT_RETRY_DELAY_SECONDS` et `ELT_DUMP_PATH` permettent de modifier le comportement du script d'ELT.
- Ajoutez de nouveaux modèles dbt dans `postgres_elt_dbt/models` pour enrichir la couche analytique.

## Tests et validation
- `docker compose run --rm dbt dbt test` : exécute les tests dbt (unicité, not null, relations).
- `docker compose logs elt_replicator` : vérifie que la réplication s'est déroulée correctement.

## Étapes suivantes possibles
- Remplacer le `pg_dump` complet par une réplication incrémentale (ex. `pg_dump --table` ou `logical replication`).
- Brancher un outil BI (Metabase, Superset) sur `destination_postgres` pour consommer les modèles dbt.
- Ajouter des sources supplémentaires (autres schémas ou S3) et les documenter dans dbt.

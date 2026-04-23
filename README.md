# pollstats

Config-driven downloader + local data store for US politics datasets, with optional ingestion into Postgres.

## Quick start

1) Start Postgres (optional):
- `docker compose -f tmp/docker-compose.yaml up -d`

2) Create a virtualenv and install deps:
- `python -m venv .venv && . .venv/bin/activate`
- `pip install -r requirements.txt`

3) List configured datasets:
- `python scripts/pollstats.py list`

4) Download/update local store (idempotent):
- `python scripts/pollstats.py update`

5) Ingest download metadata into Postgres:
- `python scripts/pollstats.py pg-init`
- `python scripts/pollstats.py pg-load`

## Data layout

- `data_store/raw/<source_id>/<dataset_id>/latest/` contains the latest fetched artifact(s)
- `data_store/raw/<source_id>/<dataset_id>/history/<timestamp>/` contains older versions (optional, controlled by config)
- `data_store/manifests/<source_id>/<dataset_id>.json` tracks ETag/Last-Modified/SHA256 so updates are idempotent

## Config

Edit:
- `config/sources.yaml` (what to download + how)
- `config/pollstats.yaml` (where to store + Postgres URL)


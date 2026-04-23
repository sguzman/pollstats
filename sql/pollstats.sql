create schema if not exists pollstats;

create table if not exists pollstats.ingest_files (
  id bigserial primary key,
  source_id text not null,
  dataset_id text not null,
  fetched_at timestamptz not null,
  url text not null,
  status text not null,
  http_status int,
  etag text,
  last_modified text,
  sha256 text,
  bytes bigint,
  local_path text not null,
  note text
);

create index if not exists ingest_files_source_dataset_fetched_at_idx
  on pollstats.ingest_files (source_id, dataset_id, fetched_at desc);


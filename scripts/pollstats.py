#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import sys
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _read_yaml(path: pathlib.Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_json(path: pathlib.Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def _sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclasses.dataclass(frozen=True)
class Dataset:
    source_id: str
    source_name: str
    dataset_id: str
    dataset_name: str
    type: str
    url: str


def _load_config() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    config_path = pathlib.Path(os.environ.get("POLLSTATS_CONFIG", "config/pollstats.yaml"))
    cfg = _read_yaml(config_path)

    sources_file = pathlib.Path(cfg.get("sources_file", "config/sources.yaml"))
    sources_cfg = _read_yaml(sources_file)
    return cfg, sources_cfg


def _iter_datasets(sources_cfg: Dict[str, Any]) -> Iterable[Dataset]:
    for source in sources_cfg.get("sources", []) or []:
        source_id = str(source["id"])
        source_name = str(source.get("name", source_id))
        for ds in source.get("datasets", []) or []:
            yield Dataset(
                source_id=source_id,
                source_name=source_name,
                dataset_id=str(ds["id"]),
                dataset_name=str(ds.get("name", ds["id"])),
                type=str(ds["type"]),
                url=str(ds["url"]),
            )


def cmd_list(_: argparse.Namespace) -> int:
    _, sources_cfg = _load_config()
    for d in _iter_datasets(sources_cfg):
        print(f"{d.source_id}/{d.dataset_id}\t{d.type}\t{d.dataset_name}\t{d.url}")
    return 0


def _manifest_path(store_dir: pathlib.Path, d: Dataset) -> pathlib.Path:
    return store_dir / "manifests" / d.source_id / f"{d.dataset_id}.json"


def _latest_dir(store_dir: pathlib.Path, d: Dataset) -> pathlib.Path:
    return store_dir / "raw" / d.source_id / d.dataset_id / "latest"


def _history_dir(store_dir: pathlib.Path, d: Dataset, stamp: str) -> pathlib.Path:
    return store_dir / "raw" / d.source_id / d.dataset_id / "history" / stamp


def _load_manifest(path: pathlib.Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _http_head(session: requests.Session, url: str, timeout_s: int) -> requests.Response:
    return session.head(url, allow_redirects=True, timeout=timeout_s)


def _http_get_to_file(session: requests.Session, url: str, out: pathlib.Path, headers: Dict[str, str], timeout_s: int) -> requests.Response:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".part")
    with session.get(url, stream=True, headers=headers, allow_redirects=True, timeout=timeout_s) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
    tmp.replace(out)
    return r


def _suggest_filename(url: str, response: Optional[requests.Response]) -> str:
    if response is not None:
        cd = response.headers.get("Content-Disposition") or ""
        # very small/forgiving parse: look for filename=
        lower = cd.lower()
        if "filename=" in lower:
            frag = cd[lower.index("filename=") + len("filename=") :].strip()
            frag = frag.strip().strip(";").strip()
            if frag.startswith(("\"", "'")) and frag.endswith(("\"", "'")) and len(frag) >= 2:
                frag = frag[1:-1]
            frag = os.path.basename(frag)
            if frag:
                return frag

    parsed = urllib.parse.urlparse(url)
    name = os.path.basename(parsed.path.rstrip("/"))
    return name or "download.bin"


def cmd_update(args: argparse.Namespace) -> int:
    cfg, sources_cfg = _load_config()
    store_dir = pathlib.Path(cfg.get("store_dir", "data_store"))
    store_dir.mkdir(parents=True, exist_ok=True)

    timeout_s = int(cfg.get("http_timeout_seconds", 60))
    user_agent = cfg.get("user_agent", "pollstats/0.1 (+local)")

    any_failed = False
    with requests.Session() as session:
        session.headers.update({"User-Agent": user_agent})
        for d in _iter_datasets(sources_cfg):
            if args.only and f"{d.source_id}/{d.dataset_id}" not in args.only:
                continue

            manifest_path = _manifest_path(store_dir, d)
            manifest = _load_manifest(manifest_path)
            fetched_at = _utc_now().isoformat()

            if d.type == "http_page":
                manifest.update(
                    {
                        "source_id": d.source_id,
                        "dataset_id": d.dataset_id,
                        "type": d.type,
                        "url": d.url,
                        "last_seen_at": fetched_at,
                    }
                )
                _write_json(manifest_path, manifest)
                print(f"SKIP(page)\t{d.source_id}/{d.dataset_id}\t{d.url}")
                continue

            if d.type != "http_file":
                print(f"ERROR\tunknown type {d.type}\t{d.source_id}/{d.dataset_id}", file=sys.stderr)
                any_failed = True
                continue

            try:
                head = _http_head(session, d.url, timeout_s=timeout_s)
                etag = head.headers.get("ETag")
                last_modified = head.headers.get("Last-Modified")
                content_length = head.headers.get("Content-Length")
            except Exception as e:
                print(f"ERROR\thead failed\t{d.source_id}/{d.dataset_id}\t{d.url}\t{e}", file=sys.stderr)
                any_failed = True
                continue

            headers: Dict[str, str] = {}
            if manifest.get("etag") and etag and manifest.get("etag") == etag:
                print(f"OK(etag)\t{d.source_id}/{d.dataset_id}\tunchanged")
                continue
            if manifest.get("last_modified") and last_modified and manifest.get("last_modified") == last_modified:
                print(f"OK(mod)\t{d.source_id}/{d.dataset_id}\tunchanged")
                continue

            if etag:
                headers["If-None-Match"] = etag
            if last_modified:
                headers["If-Modified-Since"] = last_modified

            latest_dir = _latest_dir(store_dir, d)
            latest_dir.mkdir(parents=True, exist_ok=True)
            out_file = latest_dir / "download.bin"

            try:
                r = session.get(d.url, stream=True, headers=headers, allow_redirects=True, timeout=timeout_s)
                if r.status_code == 304:
                    print(f"OK(304)\t{d.source_id}/{d.dataset_id}\tunchanged")
                    continue
                r.raise_for_status()

                suggested = _suggest_filename(d.url, r)
                out_file = latest_dir / suggested
                tmp = out_file.with_suffix(".part")
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                tmp.replace(out_file)

            except Exception as e:
                print(f"ERROR\tdownload failed\t{d.source_id}/{d.dataset_id}\t{d.url}\t{e}", file=sys.stderr)
                any_failed = True
                continue

            sha256 = _sha256_file(out_file)

            prev_sha = manifest.get("sha256")
            if prev_sha and prev_sha != sha256:
                stamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
                hist_dir = _history_dir(store_dir, d, stamp)
                hist_dir.mkdir(parents=True, exist_ok=True)
                (hist_dir / "download.bin").write_bytes(out_file.read_bytes())

            manifest.update(
                {
                    "source_id": d.source_id,
                    "dataset_id": d.dataset_id,
                    "type": d.type,
                    "url": d.url,
                    "fetched_at": fetched_at,
                    "etag": etag,
                    "last_modified": last_modified,
                    "content_length": content_length,
                    "sha256": sha256,
                    "local_path": str(out_file),
                }
            )
            _write_json(manifest_path, manifest)
            print(f"OK\t{d.source_id}/{d.dataset_id}\t{out_file}\t{sha256[:12]}")

    return 1 if any_failed else 0


def _pg_url(cfg: Dict[str, Any]) -> str:
    return os.environ.get("POLLSTATS_PG_URL", str(cfg.get("postgres_url", ""))).strip()


def cmd_pg_init(_: argparse.Namespace) -> int:
    import psycopg

    cfg, _ = _load_config()
    url = _pg_url(cfg)
    if not url:
        print("Missing postgres_url in config/pollstats.yaml (or POLLSTATS_PG_URL).", file=sys.stderr)
        return 2

    sql_path = pathlib.Path("sql/pollstats.sql")
    sql_text = sql_path.read_text(encoding="utf-8")
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    print("OK\tpg-init")
    return 0


def cmd_pg_load(args: argparse.Namespace) -> int:
    import psycopg

    cfg, sources_cfg = _load_config()
    url = _pg_url(cfg)
    if not url:
        print("Missing postgres_url in config/pollstats.yaml (or POLLSTATS_PG_URL).", file=sys.stderr)
        return 2

    store_dir = pathlib.Path(cfg.get("store_dir", "data_store"))

    rows: List[Tuple[Any, ...]] = []
    for d in _iter_datasets(sources_cfg):
        if args.only and f"{d.source_id}/{d.dataset_id}" not in args.only:
            continue
        mpath = _manifest_path(store_dir, d)
        if not mpath.exists():
            continue
        m = _load_manifest(mpath)
        rows.append(
            (
                d.source_id,
                d.dataset_id,
                _utc_now(),
                d.url,
                "manifest",
                None,
                m.get("etag"),
                m.get("last_modified"),
                m.get("sha256"),
                None,
                str(mpath),
                "manifest snapshot",
            )
        )

    if not rows:
        print("OK\tpg-load\t(no manifests found)")
        return 0

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into pollstats.ingest_files
                  (source_id, dataset_id, fetched_at, url, status, http_status, etag, last_modified, sha256, bytes, local_path, note)
                values
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                rows,
            )
        conn.commit()

    print(f"OK\tpg-load\tinserted={len(rows)}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="pollstats")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("list", help="List configured sources/datasets")
    sp.set_defaults(fn=cmd_list)

    sp = sub.add_parser("update", help="Update local data store (idempotent)")
    sp.add_argument("--only", action="append", help="Restrict to source_id/dataset_id (repeatable)")
    sp.set_defaults(fn=cmd_update)

    sp = sub.add_parser("pg-init", help="Create pollstats schema/tables")
    sp.set_defaults(fn=cmd_pg_init)

    sp = sub.add_parser("pg-load", help="Load manifest snapshots into Postgres")
    sp.add_argument("--only", action="append", help="Restrict to source_id/dataset_id (repeatable)")
    sp.set_defaults(fn=cmd_pg_load)

    args = p.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())

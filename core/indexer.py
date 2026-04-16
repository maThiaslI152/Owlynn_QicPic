"""Directory indexer for EXIF metadata and session assignment."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import exifread
import pillow_heif
import yaml
from timezonefinder import TimezoneFinder


pillow_heif.register_heif_opener()


@dataclass
class IndexedAsset:
    file_hash: str
    file_path: str
    file_name: str
    file_ext: str
    file_size: int
    captured_at_utc: str
    captured_at_local: str
    captured_at_source: str
    gps_lat: Optional[float]
    gps_lon: Optional[float]
    timezone_name: Optional[str]
    location_source: str
    ingested_at: str


def load_config(config_path: Path) -> Dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            asset_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY,
            file_hash TEXT UNIQUE NOT NULL,
            file_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_ext TEXT,
            file_size INTEGER,
            captured_at TEXT,
            captured_at_local TEXT,
            captured_at_source TEXT,
            gps_lat REAL,
            gps_lon REAL,
            timezone_name TEXT,
            location_source TEXT,
            session_id INTEGER,
            ingested_at TEXT NOT NULL,
            FOREIGN KEY(session_id) REFERENCES sessions(id)
        );

        CREATE INDEX IF NOT EXISTS idx_assets_captured_at ON assets(captured_at);
        CREATE INDEX IF NOT EXISTS idx_assets_session_id ON assets(session_id);
        CREATE INDEX IF NOT EXISTS idx_assets_file_path ON assets(file_path);
        """
    )
    conn.commit()


def hash_file(path: Path, mode: str, chunk_bytes: int) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        if mode == "chunked_sha256":
            first = handle.read(chunk_bytes)
            hasher.update(first)
            try:
                handle.seek(max(path.stat().st_size - chunk_bytes, 0))
            except OSError:
                pass
            last = handle.read(chunk_bytes)
            hasher.update(last)
        else:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
    return hasher.hexdigest()


def _ratio_to_float(value: Any) -> float:
    if hasattr(value, "num") and hasattr(value, "den"):
        return float(value.num) / float(value.den)
    return float(value)


def _gps_values_to_decimal(values: Any, ref: str) -> Optional[float]:
    try:
        d = _ratio_to_float(values.values[0])
        m = _ratio_to_float(values.values[1])
        s = _ratio_to_float(values.values[2])
        decimal = d + (m / 60.0) + (s / 3600.0)
        if ref in ("S", "W"):
            decimal = -decimal
        return decimal
    except Exception:
        return None


def read_exif(path: Path) -> Tuple[Optional[dt.datetime], str, Optional[float], Optional[float], str]:
    with path.open("rb") as handle:
        tags = exifread.process_file(handle, details=False)

    timestamp = (
        tags.get("EXIF DateTimeOriginal")
        or tags.get("EXIF DateTimeDigitized")
        or tags.get("Image DateTime")
    )
    dt_value: Optional[dt.datetime] = None
    source = "filesystem"
    if timestamp:
        raw = str(timestamp).strip()
        try:
            dt_value = dt.datetime.strptime(raw, "%Y:%m:%d %H:%M:%S")
            source = "exif"
        except ValueError:
            dt_value = None

    lat_tag = tags.get("GPS GPSLatitude")
    lat_ref = tags.get("GPS GPSLatitudeRef")
    lon_tag = tags.get("GPS GPSLongitude")
    lon_ref = tags.get("GPS GPSLongitudeRef")
    lat = _gps_values_to_decimal(lat_tag, str(lat_ref)) if lat_tag and lat_ref else None
    lon = _gps_values_to_decimal(lon_tag, str(lon_ref)) if lon_tag and lon_ref else None
    location_source = "exif" if lat is not None and lon is not None else "none"

    return dt_value, source, lat, lon, location_source


def resolve_timezone_name(
    gps_lat: Optional[float], gps_lon: Optional[float], tf: TimezoneFinder, default_tz: str
) -> str:
    if gps_lat is not None and gps_lon is not None:
        found = tf.timezone_at(lat=gps_lat, lng=gps_lon)
        if found:
            return found
    return default_tz


def normalize_timestamp(
    exif_dt: Optional[dt.datetime], path: Path, timezone_name: str
) -> Tuple[str, str, str]:
    if exif_dt is not None:
        local = exif_dt.replace(tzinfo=ZoneInfo(timezone_name))
        utc = local.astimezone(dt.timezone.utc)
        return utc.isoformat(), local.isoformat(), "exif"

    mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    return mtime.isoformat(), mtime.isoformat(), "filesystem"


def upsert_asset(conn: sqlite3.Connection, asset: IndexedAsset) -> None:
    conn.execute(
        """
        INSERT INTO assets(
            file_hash, file_path, file_name, file_ext, file_size,
            captured_at, captured_at_local, captured_at_source,
            gps_lat, gps_lon, timezone_name, location_source, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_hash) DO UPDATE SET
            file_path=excluded.file_path,
            file_name=excluded.file_name,
            file_ext=excluded.file_ext,
            file_size=excluded.file_size,
            captured_at=excluded.captured_at,
            captured_at_local=excluded.captured_at_local,
            captured_at_source=excluded.captured_at_source,
            gps_lat=excluded.gps_lat,
            gps_lon=excluded.gps_lon,
            timezone_name=excluded.timezone_name,
            location_source=excluded.location_source,
            ingested_at=excluded.ingested_at
        """,
        (
            asset.file_hash,
            asset.file_path,
            asset.file_name,
            asset.file_ext,
            asset.file_size,
            asset.captured_at_utc,
            asset.captured_at_local,
            asset.captured_at_source,
            asset.gps_lat,
            asset.gps_lon,
            asset.timezone_name,
            asset.location_source,
            asset.ingested_at,
        ),
    )


def iter_media_files(root: Path, allowed_extensions: Iterable[str]) -> Iterable[Path]:
    suffixes = {ext.lower() for ext in allowed_extensions}
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in suffixes:
            yield path


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    )
    return 2 * radius * math.asin(math.sqrt(a))


def assign_sessions(conn: sqlite3.Connection, config: Dict[str, Any]) -> int:
    session_gap_with_location = float(config["indexing"]["session_gap_hours_with_location"])
    session_gap_fallback = float(config["indexing"]["session_gap_hours_fallback"])
    location_jump_km = float(config["indexing"]["location_jump_km"])

    rows = conn.execute(
        """
        SELECT id, captured_at, gps_lat, gps_lon
        FROM assets
        WHERE captured_at IS NOT NULL
        ORDER BY captured_at ASC, file_path ASC
        """
    ).fetchall()
    if not rows:
        return 0

    conn.execute("UPDATE assets SET session_id = NULL")
    conn.execute("DELETE FROM sessions")

    session_id = 1
    prev_time = dt.datetime.fromisoformat(rows[0][1])
    prev_lat, prev_lon = rows[0][2], rows[0][3]
    conn.execute("INSERT INTO sessions(id, started_at, ended_at, asset_count) VALUES (?, ?, ?, 0)", (session_id, rows[0][1], rows[0][1]))
    conn.execute("UPDATE assets SET session_id=? WHERE id=?", (session_id, rows[0][0]))
    conn.execute("UPDATE sessions SET asset_count = asset_count + 1, ended_at=? WHERE id=?", (rows[0][1], session_id))

    for row in rows[1:]:
        asset_id, captured_at, lat, lon = row
        current_time = dt.datetime.fromisoformat(captured_at)
        hours_gap = (current_time - prev_time).total_seconds() / 3600.0

        start_new_session = False
        if prev_lat is not None and prev_lon is not None and lat is not None and lon is not None:
            distance = haversine_km(prev_lat, prev_lon, lat, lon)
            if hours_gap > session_gap_with_location or distance > location_jump_km:
                start_new_session = True
        elif hours_gap > session_gap_fallback:
            start_new_session = True

        if start_new_session:
            session_id += 1
            conn.execute(
                "INSERT INTO sessions(id, started_at, ended_at, asset_count) VALUES (?, ?, ?, 0)",
                (session_id, captured_at, captured_at),
            )

        conn.execute("UPDATE assets SET session_id=? WHERE id=?", (session_id, asset_id))
        conn.execute(
            "UPDATE sessions SET asset_count = asset_count + 1, ended_at=? WHERE id=?",
            (captured_at, session_id),
        )
        prev_time = current_time
        prev_lat, prev_lon = lat, lon

    conn.commit()
    return session_id


def run_indexing(source_dir: Path, config_path: Path = Path("config.yaml")) -> Dict[str, int]:
    config = load_config(config_path)
    db_path = Path(config["database"]["path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    init_db(conn)

    tf = TimezoneFinder()
    allowed_extensions = config["indexing"]["allowed_extensions"]
    hash_mode = config["indexing"]["hash_mode"]
    hash_chunk_bytes = int(config["indexing"]["hash_chunk_bytes"])
    default_tz = config["timezone"]["default"]

    ingested_count = 0
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    for file_path in iter_media_files(source_dir, allowed_extensions):
        file_hash = hash_file(file_path, hash_mode, hash_chunk_bytes)
        exif_dt, exif_source, gps_lat, gps_lon, location_source = read_exif(file_path)
        tz_name = resolve_timezone_name(gps_lat, gps_lon, tf, default_tz)
        captured_at_utc, captured_at_local, normalized_source = normalize_timestamp(exif_dt, file_path, tz_name)
        source = exif_source if normalized_source == "exif" else "filesystem"

        asset = IndexedAsset(
            file_hash=file_hash,
            file_path=str(file_path.resolve()),
            file_name=file_path.name,
            file_ext=file_path.suffix.lower(),
            file_size=file_path.stat().st_size,
            captured_at_utc=captured_at_utc,
            captured_at_local=captured_at_local,
            captured_at_source=source,
            gps_lat=gps_lat,
            gps_lon=gps_lon,
            timezone_name=tz_name,
            location_source=location_source,
            ingested_at=now_iso,
        )
        upsert_asset(conn, asset)
        ingested_count += 1

    conn.commit()
    session_count = assign_sessions(conn, config)
    total_assets = conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
    conn.close()

    return {"ingested": ingested_count, "assets_total": total_assets, "sessions_total": session_count}


def main() -> None:
    parser = argparse.ArgumentParser(description="Index photos into SQLite catalog.")
    parser.add_argument("source_dir", type=Path, help="Directory with media files to scan.")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path.")
    args = parser.parse_args()

    result = run_indexing(args.source_dir, args.config)
    print(result)


if __name__ == "__main__":
    main()

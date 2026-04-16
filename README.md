# Owlynn_QicPic

Local-first Python app to index, identify, and quick-pick photos from multi-day trips.

Owlynn_QicPic builds a database-first photo catalog (no file moves), groups images into sessions using EXIF time and location, and prepares the foundation for AI-assisted face clustering and quality scoring workflows.

## What it does now

- Recursively scans a source directory for image files
- Extracts EXIF date/time and GPS metadata when available
- Normalizes timestamps to UTC (default timezone fallback: `Asia/Bangkok`)
- Stores metadata in SQLite (`database/catalog.db`)
- Deduplicates by `file_hash` and updates path on rename/move
- Assigns `session_id` using time-gap and location-jump rules
- Provides a basic Streamlit UI to run indexing and inspect catalog totals

## Architecture (Split Runtime)

- **Podman/Linux scope**: Streamlit UI and SQLite file access
- **Native macOS scope**: heavy ML workers (`core/face_id.py`, `core/quality.py`) for Apple Silicon acceleration

This split keeps container workflows stable while preserving native CoreML execution provider access for future ML pipelines.

## Project layout

```text
Owlynn_QicPic/
├── app.py
├── core/
│   ├── indexer.py
│   └── runtime.py
├── database/
│   └── vector_store/
├── config.yaml
├── requirements-base.txt
├── requirements-linux.txt
├── requirements-mac.txt
├── compose.yaml
└── Containerfile
```

## Requirements

- Python 3.11+
- macOS Apple Silicon (recommended for native ML worker path)
- Podman + podman-compose (optional, for UI container flow)

## Setup

### 1) Native macOS environment (recommended for workers)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-mac.txt
```

### 2) Run Streamlit natively

```bash
streamlit run app.py
```

### 3) Run indexer from CLI

```bash
python core/indexer.py /absolute/path/to/photos --config config.yaml
```

## Podman UI flow (optional)

Build and run:

```bash
podman compose up --build
```

Streamlit will be available at `http://localhost:8501`.

Optional media mount:

```bash
MEDIA_SOURCE_DIR=/absolute/path/to/photos podman compose up --build
```

## Configuration

Key settings in `config.yaml`:

- `indexing.session_gap_hours_with_location` (default `6`)
- `indexing.session_gap_hours_fallback` (default `3`)
- `indexing.location_jump_km` (default `30`)
- `indexing.hash_mode` (`full_sha256` or `chunked_sha256`)
- `indexing.hash_chunk_bytes` (default `1048576`)
- `timezone.default` (default `Asia/Bangkok`)
- `runtime.onnx_provider_priority`

## Notes

- Original files are never moved or modified.
- In containers, ONNX providers are typically CPU-only.
- On native Apple Silicon, ONNX may expose `CoreMLExecutionProvider` depending on runtime/version.

## Roadmap

- Face embeddings + clustering pipeline (`core/face_id.py`)
- Technical and aesthetic quality scoring (`core/quality.py`)
- Quick-pick export flow (symlink set + NLE manifests)

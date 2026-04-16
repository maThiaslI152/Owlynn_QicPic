from __future__ import annotations

import sqlite3
from pathlib import Path

import streamlit as st
import yaml

from core.indexer import run_indexing
from core.runtime import get_available_providers, resolve_provider_priority


def load_config() -> dict:
    with Path("config.yaml").open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def read_catalog_counts(db_path: Path) -> dict:
    if not db_path.exists():
        return {"assets": 0, "sessions": 0}
    conn = sqlite3.connect(db_path)
    assets = conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
    sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    conn.close()
    return {"assets": assets, "sessions": sessions}


def main() -> None:
    st.set_page_config(page_title="Owlynn_QicPic", layout="wide")
    st.title("Owlynn_QicPic Indexer")
    st.caption("Database-first photo catalog for local-first trip culling.")

    config = load_config()
    db_path = Path(config["database"]["path"])

    st.subheader("Runtime Providers (Native Worker Check)")
    providers = get_available_providers()
    preferred = resolve_provider_priority(config["runtime"]["onnx_provider_priority"])
    st.write({"available": providers, "selected_priority": preferred})
    if "CoreMLExecutionProvider" in providers:
        st.success("CoreMLExecutionProvider detected (native Apple Silicon runtime).")
    else:
        st.warning("CoreMLExecutionProvider not detected. This is expected in Podman/Linux.")
    st.info("Use native macOS worker env (`requirements-mac.txt`) for face/quality acceleration.")

    st.subheader("Index Source Directory")
    source_dir_input = st.text_input("Source directory", value=str(Path.cwd()))
    if st.button("Run Indexer"):
        source = Path(source_dir_input)
        if not source.exists() or not source.is_dir():
            st.error("Invalid source directory.")
        else:
            with st.spinner("Indexing files..."):
                result = run_indexing(source)
            st.success("Indexing completed.")
            st.json(result)

    st.subheader("Catalog Summary")
    counts = read_catalog_counts(db_path)
    col1, col2 = st.columns(2)
    col1.metric("Assets", counts["assets"])
    col2.metric("Sessions", counts["sessions"])


if __name__ == "__main__":
    main()

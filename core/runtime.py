"""ONNX Runtime provider selection helpers for Apple Silicon workers."""

from __future__ import annotations

from typing import Iterable, List

import onnxruntime as ort


def get_available_providers() -> List[str]:
    """Return providers discovered in the current runtime environment."""
    return list(ort.get_available_providers())


def resolve_provider_priority(preferred: Iterable[str]) -> List[str]:
    """
    Return preferred providers filtered by current availability.

    Falls back to CPUExecutionProvider if no preferred provider is available.
    """
    available = set(get_available_providers())
    selected = [provider for provider in preferred if provider in available]
    if not selected and "CPUExecutionProvider" in available:
        selected = ["CPUExecutionProvider"]
    return selected


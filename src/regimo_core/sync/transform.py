"""Bridge to the canonical maDMP -> internal-format transformation.

The transformation logic lives in ``fuseki-proto`` (CF2-004:
``fuseki_proto.agnostic_trip.transform`` — the LinkML Map driven half that
maps a maDMP onto the internal format). regimo-core reuses it instead of
duplicating the mapping; the import is lazy so the rest of regimo-core keeps
working where ``fuseki-proto`` is not installed (tests, pure bus tooling).

Only the DMP half is used here: SMS devices/properties are *not* part of the
RDMO pull and are synced by their own socket.
"""

from __future__ import annotations

from typing import Any


class TransformError(RuntimeError):
    """Raised when a maDMP cannot be transformed into the internal format."""


def transform_madmp(document: dict[str, Any], device: dict | None = None,
                    properties: list[dict] | None = None) -> "tuple[str, dict]":
    """Transform ``{"dmp": {...}}`` (+ optional SMS extras) into Turtle.

    Returns ``(turtle, meta)`` where ``meta`` carries the project IRI (the
    maDMP ``dmp_id``) among the agnostic-trip metadata.
    """
    try:
        from fuseki_proto.agnostic_trip import transform
    except Exception as exc:  # pragma: no cover — depends on the env
        raise TransformError(
            "fuseki-proto is not importable in this environment; install it "
            "(uv pip install -e infrastructure/fuseki-proto/tool) to enable the sync engine") from exc

    try:
        turtle, _checks, meta = transform(document, device, properties or [])
    except Exception as exc:
        raise TransformError(f"agnostic-trip transform failed: {exc}") from exc

    return turtle, meta

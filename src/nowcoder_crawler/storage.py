from __future__ import annotations

import gzip
import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .identity import PageIdentity


@dataclass(frozen=True)
class StoredBody:
    path: Path
    sha256: str
    raw_bytes: int


def target_path(raw_root: Path, identity: PageIdentity) -> Path:
    directory = "feed" if identity.page_type == "feed" else "discussion"
    return raw_root / directory / f"{identity.external_id}.html.gz"


def write_gzip_atomic(raw_root: Path, identity: PageIdentity, body: bytes) -> StoredBody:
    destination = target_path(raw_root, identity)
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(body).hexdigest()
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{identity.external_id}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        ) as raw_stream:
            temporary_path = Path(raw_stream.name)
            with gzip.GzipFile(fileobj=raw_stream, mode="wb", mtime=0) as compressed:
                compressed.write(body)
        os.replace(temporary_path, destination)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
    return StoredBody(destination.resolve(), digest, len(body))

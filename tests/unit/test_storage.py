import gzip
from pathlib import Path

from nowcoder_crawler.identity import build_identity
from nowcoder_crawler.storage import target_path, write_gzip_atomic


def test_atomic_gzip_write_replaces_canonical_file(tmp_path: Path) -> None:
    identity = build_identity("feed", "a" * 32)
    first = write_gzip_atomic(tmp_path, identity, b"first")
    second = write_gzip_atomic(tmp_path, identity, b"second")
    assert first.path == second.path == target_path(tmp_path, identity).resolve()
    with gzip.open(second.path, "rb") as stream:
        assert stream.read() == b"second"
    assert second.sha256 != first.sha256
    assert list(second.path.parent.glob("*.tmp")) == []

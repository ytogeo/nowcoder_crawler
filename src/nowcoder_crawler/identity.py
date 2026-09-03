from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

FEED_RE = re.compile(r"^/feed/main/detail/([0-9a-fA-F]{32})/?$")
DISCUSSION_RE = re.compile(r"^/discuss/(\d+)/?$")
ALLOWED_HOSTS = {"nowcoder.com", "www.nowcoder.com"}


@dataclass(frozen=True)
class PageIdentity:
    page_type: str
    external_id: str
    canonical_url: str


def build_identity(page_type: str, external_id: str) -> PageIdentity:
    if page_type == "feed" and re.fullmatch(r"[0-9a-fA-F]{32}", external_id):
        normalized = external_id.lower()
        return PageIdentity(
            "feed",
            normalized,
            f"https://www.nowcoder.com/feed/main/detail/{normalized}",
        )
    if page_type == "discussion" and external_id.isdigit():
        return PageIdentity(
            "discussion",
            external_id,
            f"https://www.nowcoder.com/discuss/{external_id}",
        )
    raise ValueError(f"unsupported page identity: {page_type}/{external_id}")


def identity_from_url(url: str) -> PageIdentity | None:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_HOSTS:
        return None
    path = re.sub(r"/{2,}", "/", parsed.path)
    if match := FEED_RE.fullmatch(path):
        return build_identity("feed", match.group(1))
    if match := DISCUSSION_RE.fullmatch(path):
        return build_identity("discussion", match.group(1))
    return None


def canonicalize_url(url: str) -> str | None:
    identity = identity_from_url(url)
    return None if identity is None else identity.canonical_url


def strip_query_and_fragment(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def identity_from_experience_record(record: dict) -> PageIdentity | None:
    content_type = record.get("contentType")
    if content_type == 74:
        uuid = (record.get("momentData") or {}).get("uuid")
        if uuid:
            return build_identity("feed", str(uuid))
    if content_type == 250 and record.get("contentId") is not None:
        return build_identity("discussion", str(record["contentId"]))
    return None

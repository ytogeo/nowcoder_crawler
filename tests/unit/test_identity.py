from nowcoder_crawler.identity import (
    build_identity,
    canonicalize_url,
    identity_from_center_record,
    identity_from_url,
)


def test_feed_identity_and_canonical_url() -> None:
    uuid = "DC19D859AE77482EB9B5107067FC8A6E"
    identity = identity_from_url(
        f"https://www.nowcoder.com/feed/main/detail/{uuid}?sourceSSR=other#comment"
    )
    assert identity == build_identity("feed", uuid)
    assert identity.canonical_url.endswith(uuid.lower())


def test_discussion_identity_and_canonical_url() -> None:
    url = "https://www.nowcoder.com/discuss/921889112905224192?urlSource=sitemap"
    identity = identity_from_url(url)
    assert identity == build_identity("discussion", "921889112905224192")
    assert canonicalize_url(url) == "https://www.nowcoder.com/discuss/921889112905224192"


def test_rejects_unrelated_page() -> None:
    assert identity_from_url("https://www.nowcoder.com/users/123") is None
    assert identity_from_url("https://example.com/discuss/123") is None


def test_center_record_identity() -> None:
    feed = {"contentType": 74, "momentData": {"uuid": "a" * 32}}
    discussion = {"contentType": 250, "contentId": "123456"}
    assert identity_from_center_record(feed) == build_identity("feed", "a" * 32)
    assert identity_from_center_record(discussion) == build_identity("discussion", "123456")

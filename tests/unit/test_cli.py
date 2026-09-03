from nowcoder_crawler.cli import build_parser


def test_scheduler_full_scan_defaults_to_both_sources() -> None:
    args = build_parser().parse_args(["scheduler", "full-scan"])

    assert args.scheduler_command == "full-scan"
    assert tuple(args.sources) == ("experience-api", "sitemap")


def test_scheduler_publish_pending_has_dedicated_command() -> None:
    args = build_parser().parse_args(["scheduler", "publish-pending"])

    assert args.scheduler_command == "publish-pending"

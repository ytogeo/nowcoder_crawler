from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .config import Settings
from .scheduler import run_scheduler
from .worker import run_worker


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nowcoder public interview-post crawler")
    parser.add_argument("--verbose", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scheduler = subparsers.add_parser("scheduler", help="run one discovery/publish job")
    scheduler.add_argument(
        "--sources",
        nargs="+",
        choices=("experience-api", "sitemap"),
        default=("experience-api", "sitemap"),
    )
    scheduler.add_argument("--max-pages", type=int)

    worker = subparsers.add_parser("worker", help="consume fetch messages")
    worker.add_argument("--worker-id", required=True)
    return parser


async def _async_main(args: argparse.Namespace, settings: Settings) -> int:
    if args.command == "scheduler":
        max_pages = (
            settings.experience_api_max_pages if args.max_pages is None else args.max_pages
        )
        if max_pages < 1:
            raise ValueError("--max-pages must be positive")
        await run_scheduler(settings, sources=tuple(args.sources), max_pages=max_pages)
        return 0
    return await run_worker(settings, worker_id=args.worker_id)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    _configure_logging(args.verbose)
    try:
        settings = Settings.from_env()
        return asyncio.run(_async_main(args, settings))
    except (ValueError, RuntimeError) as exc:
        logging.getLogger("nowcoder_crawler").error("startup_failed error=%s", exc)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())

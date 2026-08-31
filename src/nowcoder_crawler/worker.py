from __future__ import annotations

import logging

import aio_pika
import httpx
from aio_pika.abc import AbstractIncomingMessage

from .config import Settings
from .database import Database
from .fetcher import (
    AttemptObservation,
    RequestPacer,
    WorkerFetchState,
    fetch_with_retry,
)
from .identity import build_identity
from .models import FetchAttempt, Page, utc_now
from .rabbit import decode_page_message
from .storage import write_gzip_atomic

LOGGER = logging.getLogger("nowcoder_crawler.worker")


async def _record_attempt(
    database: Database, page_id: int, worker_id: str, observation: AttemptObservation
) -> None:
    with database.session() as session:
        session.add(
            FetchAttempt(
                page_id=page_id,
                worker_id=worker_id,
                attempt_no=observation.attempt_no,
                started_at=observation.started_at,
                finished_at=observation.finished_at,
                outcome=observation.outcome,
                http_status=observation.http_status,
                final_url=observation.final_url,
                response_bytes=observation.response_bytes,
                elapsed_ms=observation.elapsed_ms,
                retry_after=observation.retry_after,
                error_type=observation.error_type,
                error_message=(observation.error_message or "")[:4000] or None,
            )
        )


async def handle_message(
    message: AbstractIncomingMessage,
    *,
    database: Database,
    client: httpx.AsyncClient,
    settings: Settings,
    worker_id: str,
    pacer: RequestPacer,
    fetch_state: WorkerFetchState,
) -> bool:
    try:
        page_id = decode_page_message(message.body)
    except ValueError:
        LOGGER.exception("invalid_message worker_id=%s", worker_id)
        await message.ack()
        return False

    with database.session() as session:
        page = session.get(Page, page_id)
        if page is None:
            LOGGER.error("missing_page worker_id=%s page_id=%s", worker_id, page_id)
            await message.ack()
            return False
        if page.status == "success":
            LOGGER.info("duplicate_success_ack worker_id=%s page_id=%s", worker_id, page_id)
            await message.ack()
            return False
        identity = build_identity(page.page_type, page.external_id)

    async def on_attempt(observation: AttemptObservation) -> None:
        await _record_attempt(database, page_id, worker_id, observation)
        LOGGER.info(
            "fetch_attempt worker_id=%s page_id=%s attempt=%s outcome=%s "
            "status=%s error=%s elapsed_ms=%s",
            worker_id,
            page_id,
            observation.attempt_no,
            observation.outcome,
            observation.http_status,
            observation.error_type,
            observation.elapsed_ms,
        )

    outcome = await fetch_with_retry(
        client,
        identity=identity,
        settings=settings,
        pacer=pacer,
        state=fetch_state,
        on_attempt=on_attempt,
    )
    if outcome.success:
        assert outcome.body is not None
        stored = write_gzip_atomic(settings.raw_data_dir, identity, outcome.body)
        with database.session() as session:
            page = session.get(Page, page_id)
            if page is None:
                raise RuntimeError(f"page disappeared during fetch: {page_id}")
            page.status = "success"
            page.gzip_path = str(stored.path)
            page.body_sha256 = stored.sha256
            page.http_status = outcome.http_status
            page.response_content_type = outcome.content_type
            page.response_bytes = stored.raw_bytes
            page.final_url = outcome.final_url
            page.last_fetched_at = utc_now()
            page.last_error_type = None
            page.last_error_message = None
        LOGGER.info(
            "fetch_success_committed worker_id=%s page_id=%s path=%s",
            worker_id,
            page_id,
            stored.path,
        )
        if settings.failpoint_after_success_commit:
            raise RuntimeError("FAILPOINT_AFTER_SUCCESS_COMMIT")
        await message.ack()
        LOGGER.info("message_acked worker_id=%s page_id=%s result=success", worker_id, page_id)
        return False

    with database.session() as session:
        page = session.get(Page, page_id)
        if page is None:
            raise RuntimeError(f"page disappeared during failure update: {page_id}")
        page.status = "failed"
        page.last_error_type = outcome.error_type or "retryable"
        page.last_error_message = (outcome.error_message or "request failed")[:4000]
        page.http_status = outcome.http_status
        page.final_url = outcome.final_url
    await message.ack()
    LOGGER.warning(
        "message_acked worker_id=%s page_id=%s result=failed error_type=%s",
        worker_id,
        page_id,
        outcome.error_type,
    )
    return outcome.exit_worker


async def run_worker(settings: Settings, *, worker_id: str) -> int:
    database = Database(settings.mysql_dsn)
    database.create_schema()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=settings.worker_prefetch)
    queue = await channel.declare_queue(settings.fetch_queue, durable=True)
    pacer = RequestPacer(settings.fetch_base_delay_seconds, settings.fetch_jitter_seconds)
    fetch_state = WorkerFetchState()
    timeout = httpx.Timeout(
        settings.fetch_timeout_seconds,
        connect=settings.fetch_connect_timeout_seconds,
    )
    try:
        async with httpx.AsyncClient(
            timeout=timeout, follow_redirects=True, max_redirects=5
        ) as client:
            async with queue.iterator() as iterator:
                LOGGER.info(
                    "worker_started worker_id=%s queue=%s prefetch=%s concurrency=1",
                    worker_id,
                    settings.fetch_queue,
                    settings.worker_prefetch,
                )
                async for message in iterator:
                    should_exit = await handle_message(
                        message,
                        database=database,
                        client=client,
                        settings=settings,
                        worker_id=worker_id,
                        pacer=pacer,
                        fetch_state=fetch_state,
                    )
                    if should_exit:
                        LOGGER.error("worker_blocked_exit worker_id=%s", worker_id)
                        return 2
    finally:
        await connection.close()
        database.close()
    return 0

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractRobustConnection, AbstractRobustQueue


def encode_page_message(page_id: int) -> bytes:
    return json.dumps({"schema_version": 1, "page_id": page_id}, separators=(",", ":")).encode(
        "utf-8"
    )


def decode_page_message(body: bytes) -> int:
    try:
        value: dict[str, Any] = json.loads(body)
        if value.get("schema_version") != 1:
            raise ValueError("unsupported schema_version")
        page_id = int(value["page_id"])
        if page_id < 1:
            raise ValueError("page_id must be positive")
        return page_id
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid fetch message: {exc}") from exc


@dataclass
class RabbitPublisher:
    connection: AbstractRobustConnection
    queue: AbstractRobustQueue

    @classmethod
    async def connect(cls, url: str, queue_name: str) -> RabbitPublisher:
        connection = await aio_pika.connect_robust(url)
        channel = await connection.channel(publisher_confirms=True)
        queue = await channel.declare_queue(queue_name, durable=True)
        return cls(connection=connection, queue=queue)

    async def publish_page(self, page_id: int) -> None:
        await self.queue.channel.default_exchange.publish(
            Message(
                encode_page_message(page_id),
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type="application/json",
                type="fetch-page-v1",
            ),
            routing_key=self.queue.name,
        )

    async def close(self) -> None:
        await self.connection.close()

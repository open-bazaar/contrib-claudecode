"""Publish every native stream-json line before projection or routing."""

from __future__ import annotations

from time import monotonic_ns, time_ns
from typing import Literal
from uuid import uuid7

from bazaar_contrib_audit import Audit, AuditContext
from bazaar_contrib_harness import HarnessSourceEvent


class SourceAudit:
    def __init__(self, audit: Audit, context: AuditContext):
        self.audit = audit
        self.context = context
        self.id = str(uuid7())
        self.sequence = 0
        self.turn_id: str | None = None
        self.closed = False

    async def publish(self, data: str, direction: Literal["sent", "received"]) -> None:
        occurred_at_ms = time_ns() // 1_000_000
        self.sequence += 1
        event = HarnessSourceEvent(
            provider="claudecode",
            source_id=self.id,
            sequence=self.sequence,
            direction=direction,
            data=data,
            occurred_at_ms=occurred_at_ms,
            monotonic_ns=monotonic_ns(),
        )
        await self.audit.publish(
            f"harness.source.{direction}",
            context=self.context.model_copy(update={"turn_id": self.turn_id}),
            payload=event.model_dump(mode="json"),
            occurred_at_ms=occurred_at_ms,
        )

    async def close(self, returncode: int | None) -> None:
        if self.closed:
            return
        self.closed = True
        await self.audit.publish(
            "harness.source.closed",
            context=self.context,
            payload={
                "provider": "claudecode",
                "source_id": self.id,
                "sequence": self.sequence,
                "returncode": returncode,
            },
        )

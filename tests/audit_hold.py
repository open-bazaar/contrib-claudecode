"""Offer AUDIT through a hook that the publishing harness awaits."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from bazaar_contrib_audit import AUDIT, Audit, AuditContext, AuditError, NodeAudit
from bazaar_core import Context, Contrib
from pydantic import JsonValue

type Hold = Callable[[str, AuditContext, JsonValue], Awaitable[None]]


class HeldAudit:
    def __init__(self, audit: Audit, hold: Hold):
        self.audit = audit
        self.hold = hold

    async def publish(
        self,
        event_name: str,
        *,
        context: AuditContext,
        payload: JsonValue,
        occurred_at_ms: int | None = None,
        error: AuditError | None = None,
    ) -> None:
        await self.hold(event_name, context, payload)
        await self.audit.publish(
            event_name,
            context=context,
            payload=payload,
            occurred_at_ms=occurred_at_ms,
            error=error,
        )


def held_audit(hold: Hold) -> Contrib:
    @asynccontextmanager
    async def lifespan(ctx: Context):
        ctx.offer(AUDIT, HeldAudit(NodeAudit(ctx), hold))
        yield

    return Contrib(offers=[AUDIT], lifespan=lifespan)

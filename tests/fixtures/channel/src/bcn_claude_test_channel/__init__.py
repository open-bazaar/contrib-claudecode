"""A real TCP Channel for isolated Claude Code bcc acceptance."""

# Provider methods implement the public Harness call budgets.
# ruff: noqa: ASYNC109

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing, asynccontextmanager
from functools import partial
from pathlib import Path
from time import time_ns
from typing import Any, ClassVar, Literal, cast
from uuid import NAMESPACE_URL, uuid5, uuid7

import aiofiles
from bazaar_contrib_audit import AUDIT, AUDIT_EVENT, AuditContext, AuditEvent
from bazaar_contrib_channel import (
    CHANNEL,
    CHANNEL_BUILDERS,
    CHANNEL_INPUT,
    ChannelApprovalRequest,
    ChannelBuilderConfig,
    ChannelConnectionError,
    ChannelDeliveryReceipt,
    ChannelError,
    ChannelErrorDetail,
    ChannelIdentity,
    ChannelInput,
    ChannelSendAttempt,
    ChannelSendPart,
    ChannelSendRequest,
    ChannelTarget,
    ChannelTargetPresentation,
    DmAddress,
    DmRequest,
    InboundAttachment,
    InboundMessage,
    SenderIdentity,
    channel_builder_key,
)
from bazaar_contrib_harness import ApprovalResult, HarnessOutput, harness_builder_key
from bazaar_contrib_timer import TIMER, Timer
from bazaar_contrib_timer import timeout as timer_timeout
from bazaar_core import Context, Contrib, ServiceKey
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


class ChannelConfig(BaseModel):
    kind: ClassVar[str] = "test-channel"
    model_config = ConfigDict(extra="forbid", frozen=True)
    endpoint_file: str = "channel.json"
    audit_file: Path


class AuditJournal:
    def __init__(self):
        self.paths: dict[str, Path] = {}
        self.lock = asyncio.Lock()


JOURNAL = ServiceKey[AuditJournal]("claude-test.audit-journal")


async def persist(ctx: Context, event: AuditEvent):
    journal = ctx.accept(JOURNAL)
    path = journal.paths.get(event.context.agent_id)
    if path is not None:
        async with journal.lock, aiofiles.open(path, "a") as stream:
            await stream.write(event.model_dump_json() + "\n")


class Attachment(BaseModel):
    """A source file supplied by the TCP control peer."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str
    kind: str = "file"
    state: Literal["ready", "failed"] = "ready"
    path: Path | None = None
    gate: str | None = None
    media_type: str | None = None
    error: str | None = None

    @model_validator(mode="after")
    def source(self):
        if self.state == "ready" and self.path is None:
            raise ValueError("a ready attachment requires a local source")
        if self.state == "failed" and self.path is not None:
            raise ValueError("a failed attachment has no local source")
        return self


async def file_chunks(
    path: Path, gate: asyncio.Event | None = None
) -> AsyncGenerator[bytes]:
    """Read the peer's source lazily through the public attachment contract."""
    if gate is not None:
        await gate.wait()
    stream = await asyncio.to_thread(path.open, "rb")
    try:
        while chunk := await asyncio.to_thread(stream.read, 64 * 1024):
            yield chunk
    finally:
        await asyncio.to_thread(stream.close)


class ChannelMessage(BaseModel):
    """The test peer's wire message before the provider assigns local identities."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    target: ChannelTarget
    sender: SenderIdentity
    body: str
    message_type: str = "text"
    target_presentation: ChannelTargetPresentation | None = None
    reply_to_message_id: str | None = None
    received_at_ms: int | None = None
    provider_payload_ref: str | None = None
    attachments: tuple[Attachment, ...] = ()
    occurred_at_ms: int | None = Field(default_factory=lambda: time_ns() // 1_000_000)
    provider_chat_id: str | None = None
    notifies_runtime: bool = True
    mentions_agent: bool = False
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class TestChannel:
    """A connected TCP channel controlled by a peer that observes and approves."""

    def __init__(
        self, input: ChannelInput, group: asyncio.TaskGroup, kind: str, timer: Timer
    ):
        self.input = input
        self.group = group
        self.kind = kind
        self.timer = timer
        self.writer: asyncio.StreamWriter | None = None
        self.connected = asyncio.Event()
        self.inbound: asyncio.Queue[InboundMessage] = asyncio.Queue()
        self.outbound: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.downloads: dict[str, asyncio.Event] = {}

    def get_identity(self) -> ChannelIdentity:
        return ChannelIdentity(id=self.input.agent_id, name=self.input.agent_name)

    def backfill_provider_thread_id(
        self, provider_thread_id: str, *, channel: str, channel_identity: str
    ) -> str:
        return provider_thread_id

    def dm_address(self, request: DmRequest) -> DmAddress | None:
        if request.sender_id is None:
            return None
        identity = f"{self.kind}:dm:{request.sender_id}"
        return DmAddress(
            channel_session_id=str(uuid5(NAMESPACE_URL, identity + ":channel")),
            thread_id=str(uuid5(NAMESPACE_URL, identity + ":thread")),
            provider_thread_id=request.sender_id,
            channel_identity=request.channel_identity,
        )

    def connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.writer = writer
        self.connected.set()
        self.group.create_task(self.connection(reader, writer))

    async def connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            while data := await reader.readline():
                packet = json.loads(data)
                if "inbound" in packet:
                    message = ChannelMessage.model_validate(packet["inbound"])
                    identity = (
                        f"{self.kind}:{message.target.kind}:{message.target.thread_id}"
                    )
                    await self.inbound.put(
                        InboundMessage(
                            **message.model_dump(exclude={"id", "attachments"}),
                            attachments=tuple(
                                InboundAttachment(
                                    **attachment.model_dump(exclude={"path", "gate"}),
                                    source=aclosing(
                                        file_chunks(
                                            cast(Path, attachment.path),
                                            self.downloads.setdefault(
                                                attachment.gate, asyncio.Event()
                                            )
                                            if attachment.gate is not None
                                            else None,
                                        )
                                    )
                                    if attachment.state == "ready"
                                    else None,
                                )
                                for attachment in message.attachments
                            ),
                            id=str(
                                uuid5(
                                    NAMESPACE_URL,
                                    f"{self.kind}:{self.input.agent_id}:message:{message.id}",
                                )
                            ),
                            provider_message_id=message.id,
                            channel_session_id=str(
                                uuid5(NAMESPACE_URL, identity + ":channel")
                            ),
                            thread_id=str(uuid5(NAMESPACE_URL, identity + ":thread")),
                        )
                    )
                elif "release_attachment" in packet:
                    self.downloads.pop(packet["release_attachment"]).set()
                else:
                    pending = self.pending.pop(packet["id"], None)
                    if pending is not None and not pending.done():
                        pending.set_result(packet)
        finally:
            if self.writer is writer:
                self.writer = None
                self.connected.clear()
                for pending in self.pending.values():
                    if not pending.done():
                        pending.set_exception(
                            ChannelConnectionError(
                                "channel peer disconnected before acknowledgement",
                                phase="acknowledge",
                            )
                        )
                self.pending.clear()
            writer.close()
            await writer.wait_closed()

    async def pump(self):
        while True:
            packet = await self.outbound.get()
            while self.writer is None:
                await self.connected.wait()
            writer = self.writer
            writer.write(json.dumps(packet).encode() + b"\n")
            await writer.drain()
            self.outbound.task_done()

    async def receive(self) -> AsyncIterator[InboundMessage]:
        while True:
            yield await self.inbound.get()

    def accept_turn_event(self, event: HarnessOutput, *, target: ChannelTarget):
        self.outbound.put_nowait(
            {
                "event": event.model_dump(mode="json"),
                "target": target.model_dump(mode="json"),
            }
        )

    async def prepare_send(
        self, request: ChannelSendRequest
    ) -> tuple[ChannelSendPart, ...]:
        requests = [
            request.model_copy(
                update={"body": request.body[start : start + 4096], "attachments": ()}
            )
            for start in range(0, len(request.body), 4096)
        ]
        requests.extend(
            request.model_copy(update={"body": "", "attachments": (attachment,)})
            for attachment in request.attachments
        )
        return tuple(
            ChannelSendPart(
                send=partial(self.send_part, request, index, len(requests)),
                message=request,
            )
            for index, request in enumerate(requests)
        )

    async def send_part(
        self,
        request: ChannelSendRequest,
        index: int,
        count: int,
        attempt: ChannelSendAttempt,
    ) -> ChannelDeliveryReceipt:
        writer = self.writer
        if writer is None or writer.is_closing():
            raise ChannelConnectionError(
                "channel peer is disconnected", phase="connect", delivery="not_sent"
            )
        id = str(uuid7())
        self.pending[id] = asyncio.get_running_loop().create_future()
        try:
            writer.write(
                json.dumps(
                    {
                        "id": id,
                        "operation_id": attempt.id,
                        "attempt": attempt.number,
                        "part": {"index": index, "count": count},
                        "outbound": request.model_dump(mode="json"),
                    }
                ).encode()
                + b"\n"
            )
            await writer.drain()
            packet = await self.pending[id]
        finally:
            self.pending.pop(id, None)
        if "error" in packet:
            detail = ChannelErrorDetail.model_validate(packet["error"])
            error = ChannelError(
                detail.message,
                phase=detail.phase,
                delivery=detail.delivery,
                retryable=detail.retryable,
                code=detail.code,
                retry_after=detail.retry_after,
            )
            error.detail = detail
            raise error
        return (
            ChannelDeliveryReceipt.model_validate(packet["receipt"])
            if "receipt" in packet
            else ChannelDeliveryReceipt(status="confirmed", provider_message_id=id)
        )

    async def request_approval(
        self, request: ChannelApprovalRequest, *, timeout: float
    ) -> ApprovalResult:
        id = request.approval.request_id
        self.pending[id] = asyncio.get_running_loop().create_future()
        await self.outbound.put({"id": id, "approval": request.model_dump(mode="json")})
        try:
            packet = await self.pending[id]
            return ApprovalResult.model_validate(packet["decision"])
        finally:
            self.pending.pop(id, None)


def record(ctx: Context, event: AuditEvent):
    channel = cast(TestChannel, ctx.accept(CHANNEL))
    if event.context.agent_id == channel.input.agent_id:
        channel.outbound.put_nowait({"audit": event.model_dump(mode="json")})


@asynccontextmanager
async def channel_lifespan(ctx: Context, config: ChannelConfig):
    input = ctx.accept(CHANNEL_INPUT)
    ctx.accept(JOURNAL).paths[input.agent_id] = config.audit_file
    async with timer_timeout(ctx.accept(TIMER), 30_000):
        inspection = await ctx.accept(harness_builder_key("claudecode")).inspect(
            input.workspace, timeout=30
        )
    await ctx.accept(AUDIT).publish(
        "test_channel.inspection",
        context=AuditContext(agent_id=input.agent_id, channel=config.kind),
        payload=inspection.model_dump(mode="json"),
    )
    async with asyncio.TaskGroup() as group:
        channel = TestChannel(input, group, config.kind, ctx.accept(TIMER))

        server = await asyncio.start_server(channel.connect, "127.0.0.1", 0)
        pump = group.create_task(channel.pump())
        endpoint = input.workspace / config.endpoint_file
        temporary = endpoint.with_suffix(".tmp")
        await asyncio.to_thread(
            temporary.write_text,
            json.dumps(
                {
                    "port": server.sockets[0].getsockname()[1],
                    "agent_id": input.agent_id,
                }
            ),
        )
        await asyncio.to_thread(temporary.replace, endpoint)
        ctx.offer(CHANNEL, channel)
        try:
            yield
        finally:
            server.close()
            if channel.writer is not None:
                await channel.outbound.join()
                channel.writer.close()
                await channel.writer.wait_closed()
            pump.cancel()
            await server.wait_closed()
            await asyncio.to_thread(
                endpoint.with_suffix(".closed").write_text, input.agent_id
            )


channel = Contrib(
    config=ChannelConfig,
    accepts=[CHANNEL_INPUT, TIMER, harness_builder_key("claudecode"), JOURNAL, AUDIT],
    offers=[CHANNEL],
    lifespan=channel_lifespan,
    listen={AUDIT_EVENT: record},
)


class ChannelBuilderParameters(ChannelBuilderConfig):
    kind: ClassVar[str] = "test-channel"
    entity_model: ClassVar[type[BaseModel]] = ChannelConfig
    entry: ClassVar[Contrib[Any]] = channel


@asynccontextmanager
async def channel_builder_lifespan(ctx: Context, config: ChannelBuilderParameters):
    ctx.offer(JOURNAL, AuditJournal())
    with ctx.accept(CHANNEL_BUILDERS).register(config.builder()) as builder:
        ctx.offer(channel_builder_key(builder.kind), builder)
        yield


channel_builder = Contrib(
    config=ChannelBuilderParameters,
    accepts=[CHANNEL_BUILDERS],
    offers=[channel_builder_key("test-channel"), JOURNAL],
    lifespan=channel_builder_lifespan,
    listen={AUDIT_EVENT: persist},
)

"""Register the ClaudeCode builder and its privately composed Harness."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, ClassVar

from bazaar_contrib_audit import AUDIT
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_BUILDERS,
    HARNESS_INPUT,
    HarnessBuilderConfig,
    HarnessInspection,
    harness_builder_key,
)
from bazaar_contrib_timer import TIMER
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict

from .inspection import inspect
from .runtime import ClaudeCodeHarness


class Config(BaseModel):
    model_config = ConfigDict(frozen=True)
    model: str | None = None
    effort: str | None = None
    api_key: str | None = None


@asynccontextmanager
async def lifespan(ctx: Context, config: Config):
    instance = ClaudeCodeHarness(
        ctx.accept(HARNESS_INPUT),
        ctx.accept(TIMER),
        ctx.accept(AUDIT),
        config.model,
        config.effort,
        config.api_key,
    )
    ctx.offer(HARNESS, instance)
    try:
        yield
    finally:
        await instance.aclose()


harness = Contrib(
    config=Config,
    accepts=[HARNESS_INPUT, TIMER, AUDIT],
    offers=[HARNESS],
    lifespan=lifespan,
)


class BuilderConfig(HarnessBuilderConfig):
    kind: ClassVar[str] = "claudecode"
    entity_model: ClassVar[type[BaseModel]] = Config
    entry: ClassVar[Contrib[Any]] = harness

    @classmethod
    async def inspect(cls, workspace: Path, *, timeout: float) -> HarnessInspection:  # noqa: ASYNC109
        return await inspect(workspace, timeout=timeout)


@asynccontextmanager
async def builder_lifespan(ctx: Context, config: BuilderConfig):
    with ctx.accept(HARNESS_BUILDERS).register(config.builder()) as builder:
        ctx.offer(harness_builder_key(builder.kind), builder)
        yield


builder = Contrib(
    config=BuilderConfig,
    accepts=[HARNESS_BUILDERS],
    offers=[harness_builder_key("claudecode")],
    lifespan=builder_lifespan,
)

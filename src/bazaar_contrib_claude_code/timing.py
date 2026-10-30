"""Keep the CLI's existing deadlines on the composed timer service."""

from __future__ import annotations

import asyncio
from math import ceil

from bazaar_contrib_timer import Timer
from bazaar_contrib_timer import timeout as timer_timeout


def timeout(timer: Timer | None, seconds: float):
    # Builder inspection runs before a Harness and its services are composed.
    return (
        asyncio.timeout(seconds)
        if timer is None
        else timer_timeout(timer, ceil(seconds * 1000))
    )

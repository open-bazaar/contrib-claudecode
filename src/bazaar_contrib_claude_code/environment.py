"""The node environment inherited by Claude inspection and runtime processes."""

import os


def environment() -> dict[str, str]:
    return os.environ.copy()

from __future__ import annotations

import asyncio

_READ_CHUNK_BYTES = 64 * 1024


class UnlimitedLineReader:
    """Read newline-delimited bytes without asyncio's implicit line limit."""

    def __init__(self, reader: asyncio.StreamReader) -> None:
        self._reader = reader
        self._buffer = bytearray()
        self._eof = False

    async def readline(self) -> bytes:
        while True:
            boundary = self._buffer.find(b"\n")
            if boundary >= 0:
                boundary += 1
                line = bytes(self._buffer[:boundary])
                del self._buffer[:boundary]
                return line
            if self._eof:
                if not self._buffer:
                    return b""
                line = bytes(self._buffer)
                self._buffer.clear()
                return line
            chunk = await self._reader.read(_READ_CHUNK_BYTES)
            if chunk:
                self._buffer.extend(chunk)
            else:
                self._eof = True


__all__ = ["UnlimitedLineReader"]

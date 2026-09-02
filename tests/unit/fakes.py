"""Hand-written test doubles.

Deliberately hand-written rather than ``unittest.mock``: these fakes encode
what the real dependency actually does (librdkafka's queue raises BufferError
when full; aiohttp's ``get`` is an async context manager returning a response
with headers), and a MagicMock encodes nothing at all. When librdkafka changes
its contract, a fake that models the contract is the thing that tells us.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import Any

__all__ = ["FakeProducer", "FakeResponse", "FakeSession", "FakeSocket", "ScriptedConnect"]


class FakeProducer:
    """Models the slice of ``confluent_kafka.Producer`` we depend on."""

    def __init__(self, *, queue_limit: int | None = None, fail_delivery: bool = False) -> None:
        self.messages: list[dict[str, Any]] = []
        self.flush_calls = 0
        self._queue_limit = queue_limit
        self._fail_delivery = fail_delivery

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self._queue_limit is not None and len(self.messages) >= self._queue_limit:
            raise BufferError("Local: Queue full")
        self.messages.append({"topic": topic, **kwargs})
        callback = kwargs.get("on_delivery")
        if callback is not None:
            callback(_FakeKafkaError("_MSG_TIMED_OUT") if self._fail_delivery else None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_calls += 1
        return 0

    def __len__(self) -> int:
        return len(self.messages)

    def topics(self) -> list[str]:
        return [message["topic"] for message in self.messages]


class _FakeKafkaError:
    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return f"KafkaError{{code={self._name}}}"


class FakeSocket:
    """A websocket that yields a scripted sequence then behaves as instructed.

    ``after`` decides what happens once the script is exhausted: ``"close"``
    ends the iteration cleanly, ``"hang"`` blocks forever (which is how the
    idle-watchdog test reproduces a silent socket), and an exception instance
    is raised.
    """

    def __init__(self, messages: Iterable[str], *, after: Any = "close") -> None:
        self._messages = list(messages)
        self._after = after
        self.closed = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[str]:
        for message in self._messages:
            yield message
        if isinstance(self._after, BaseException):
            raise self._after
        if self._after == "hang":
            await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> FakeSocket:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()


class ScriptedConnect:
    """Hands out a queued socket per connection attempt, then raises.

    Lets a test drive the full reconnect state machine deterministically: first
    connection drops, second succeeds, third is refused.
    """

    def __init__(self, sockets: Iterable[Any]) -> None:
        self._sockets = list(sockets)
        self.urls: list[str] = []

    def __call__(self, url: str) -> Any:
        self.urls.append(url)
        if not self._sockets:
            raise ConnectionRefusedError("no more scripted sockets")
        socket = self._sockets.pop(0)
        if isinstance(socket, BaseException):
            raise socket
        return socket

    @property
    def attempts(self) -> int:
        return len(self.urls)


class FakeResponse:
    """Models an ``aiohttp`` response used as an async context manager."""

    def __init__(
        self,
        *,
        status: int = 200,
        payload: Any = None,
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self._payload = payload
        self._text = text

    async def json(self) -> Any:
        return self._payload

    async def text(self) -> str:
        return self._text or str(self._payload)

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        return None


class FakeSession:
    """Serves queued responses and records the requests that asked for them."""

    def __init__(self, responses: Iterable[FakeResponse]) -> None:
        self._responses = list(responses)
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def get(self, url: str, params: dict[str, Any] | None = None) -> FakeResponse:
        self.requests.append((url, params or {}))
        if not self._responses:
            raise AssertionError(f"unexpected request to {url} with {params}")
        return self._responses.pop(0)

    async def close(self) -> None:
        self.closed = True

import inspect
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import trio

from ._ipc import Channel


_context: ContextVar['Context'] = ContextVar('context')


@dataclass(frozen=True)
class Context:
    """An IAC (inter-actor communication) context.

    Allows maintaining task or protocol specific state between communicating
    actors. A unique context is created on the receiving end for every request
    to a remote actor.
    """
    chan: Channel
    cid: str
    cancel_scope: trio.CancelScope

    async def send_yield(self, data: Any) -> None:
        await self.chan.send({'yield': data, 'cid': self.cid})

    async def send_stop(self) -> None:
        await self.chan.send({'stop': True, 'cid': self.cid})


def current_context():
    """Get the current task's context instance.
    """
    return _context.get()


def stream(func):
    """Mark an async function as a streaming routine with ``@stream``.
    """
    func._tractor_stream_function = True
    sig = inspect.signature(func)
    params = sig.parameters
    if 'ctx' not in params and 'to_trio' not in params:
        raise TypeError(
            "The first argument to the stream function "
            f"{func.__name__} must be `ctx: tractor.Context` "
            "(Or ``to_trio`` if using ``asyncio`` in guest mode)."
        )
    return func

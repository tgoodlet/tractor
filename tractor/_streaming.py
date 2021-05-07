"""
Message stream types and APIs.

"""
import inspect
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any, Iterator, Optional, Callable,
    AsyncGenerator,
)

import warnings

import trio

from ._ipc import Channel
from ._exceptions import unpack_error
from ._state import current_actor
from .log import get_logger


log = get_logger(__name__)


# TODO: generic typing like trio's receive channel
# but with msgspec messages?
# class ReceiveChannel(AsyncResource, Generic[ReceiveType]):


class ReceiveMsgStream(trio.abc.ReceiveChannel):
    """A wrapper around a ``trio._channel.MemoryReceiveChannel`` with
    special behaviour for signalling stream termination across an
    inter-actor ``Channel``. This is the type returned to a local task
    which invoked a remote streaming function using `Portal.run()`.

    Termination rules:
    - if the local task signals stop iteration a cancel signal is
      relayed to the remote task indicating to stop streaming
    - if the remote task signals the end of a stream, raise a
      ``StopAsyncIteration`` to terminate the local ``async for``

    """
    def __init__(
        self,
        ctx: 'Context',  # typing: ignore # noqa
        rx_chan: trio.abc.ReceiveChannel,
        shield: bool = False,
    ) -> None:
        self._ctx = ctx
        self._rx_chan = rx_chan
        self._shielded = shield

    # delegate directly to underlying mem channel
    def receive_nowait(self):
        return self._rx_chan.receive_nowait()

    async def receive(self):
        try:
            msg = await self._rx_chan.receive()
            return msg['yield']

        except KeyError:
            # internal error should never get here
            assert msg.get('cid'), ("Received internal error at portal?")

            # TODO: handle 2 cases with 3.10 match syntax
            # - 'stop'
            # - 'error'
            # possibly just handle msg['stop'] here!

            if msg.get('stop'):
                log.debug(f"{self} was stopped at remote end")
                # when the send is closed we assume the stream has
                # terminated and signal this local iterator to stop
                await self.aclose()
                raise trio.EndOfChannel

            # TODO: test that shows stream raising an expected error!!!
            elif msg.get('error'):
                # raise the error message
                raise unpack_error(msg, self._ctx.chan)

            else:
                raise

        except (trio.ClosedResourceError, StopAsyncIteration):
            # XXX: this indicates that a `stop` message was
            # sent by the far side of the underlying channel.
            # Currently this is triggered by calling ``.aclose()`` on
            # the send side of the channel inside
            # ``Actor._push_result()``, but maybe it should be put here?
            # to avoid exposing the internal mem chan closing mechanism?
            # in theory we could instead do some flushing of the channel
            # if needed to ensure all consumers are complete before
            # triggering closure too early?

            # Locally, we want to close this stream gracefully, by
            # terminating any local consumers tasks deterministically.
            # We **don't** want to be closing this send channel and not
            # relaying a final value to remaining consumers who may not
            # have been scheduled to receive it yet?

            # lots of testing to do here

            # when the send is closed we assume the stream has
            # terminated and signal this local iterator to stop
            await self.aclose()
            # await self._ctx.send_stop()
            raise StopAsyncIteration

        except trio.Cancelled:
            # relay cancels to the remote task
            await self.aclose()
            raise

    @contextmanager
    def shield(
        self
    ) -> Iterator['ReceiveMsgStream']:  # noqa
        """Shield this stream's underlying channel such that a local consumer task
        can be cancelled (and possibly restarted) using ``trio.Cancelled``.

        Note that here, "shielding" here guards against relaying
        a ``'stop'`` message to the far end of the stream thus keeping
        the stream machinery active and ready for further use, it does
        not have anything to do with an internal ``trio.CancelScope``.

        """
        self._shielded = True
        yield self
        self._shielded = False

    async def aclose(self):
        """Cancel associated remote actor task and local memory channel
        on close.

        """
        # TODO: proper adherance to trio's `.aclose()` semantics:
        # https://trio.readthedocs.io/en/stable/reference-io.html#trio.abc.AsyncResource.aclose
        rx_chan = self._rx_chan

        if rx_chan._closed:
            log.warning(f"{self} is already closed")
            return

        # TODO: broadcasting to multiple consumers
        # stats = rx_chan.statistics()
        # if stats.open_receive_channels > 1:
        #     # if we've been cloned don't kill the stream
        #     log.debug(
        #       "there are still consumers running keeping stream alive")
        #     return

        if self._shielded:
            log.warning(f"{self} is shielded, portal channel being kept alive")
            return

        # NOTE: this is super subtle IPC messaging stuff:
        # Relay stop iteration to far end **iff** we're
        # in bidirectional mode. If we're only streaming
        # *from* one side then that side **won't** have an
        # entry in `Actor._cids2qs` (maybe it should though?).
        # So any `yield` or `stop` msgs sent from the caller side
        # will cause key errors on the callee side since there is
        # no entry for a local feeder mem chan since the callee task
        # isn't expecting messages to be sent by the caller.
        # Thus, we must check that this context DOES NOT
        # have a portal reference to ensure this is indeed the callee
        # side and can relay a 'stop'. In the bidirectional case,
        # `Context.open_stream()` will create the `Actor._cids2qs`
        # entry from a call to `Actor.get_memchans()`.
        if not self._ctx._portal:
            # only for 2 way streams can we can send
            # stop from the caller side
            await self._ctx.send_stop()

        # close the local mem chan
        await rx_chan.aclose()

    # TODO: but make it broadcasting to consumers
    # def clone(self):
    #     """Clone this receive channel allowing for multi-task
    #     consumption from the same channel.

    #     """
    #     return ReceiveStream(
    #         self._cid,
    #         self._rx_chan.clone(),
    #         self._portal,
    #     )


class MsgStream(ReceiveMsgStream, trio.abc.Channel):
    """
    Bidirectional message stream for use within an inter-actor actor
    ``Context```.

    """
    async def send(
        self,
        data: Any
    ) -> None:
        await self._ctx.chan.send({'yield': data, 'cid': self._ctx.cid})


@dataclass(frozen=True)
class Context:
    """An IAC (inter-actor communication) context.

    Allows maintaining task or protocol specific state between communicating
    actors. A unique context is created on the receiving end for every request
    to a remote actor.

    A context can be cancelled and (eventually) restarted from
    either side of the underlying IPC channel.

    A context can be used to open task oriented message streams.

    """
    chan: Channel
    cid: str

    # TODO: should we have seperate types for caller vs. callee
    # side contexts? The caller always opens a portal whereas the callee
    # is always responding back through a context-stream

    # only set on the caller side
    _portal: Optional['Portal'] = None    # type: ignore # noqa

    # only set on the callee side
    _cancel_scope: Optional[trio.CancelScope] = None

    async def send_yield(self, data: Any) -> None:

        warnings.warn(
            "`Context.send_yield()` is now deprecated. "
            "Use ``MessageStream.send()``. ",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.chan.send({'yield': data, 'cid': self.cid})

    async def send_stop(self) -> None:
        await self.chan.send({'stop': True, 'cid': self.cid})

    async def cancel(self) -> None:
        """Cancel this inter-actor-task context.

        Request that the far side cancel it's current linked context,
        timeout quickly to sidestep 2-generals...

        """
        if self._portal:  # caller side:
            if not self._portal:
                raise RuntimeError(
                    "No portal found, this is likely a callee side context"
                )

            cid = self.cid
            with trio.move_on_after(0.5) as cs:
                cs.shield = True
                log.warning(
                    f"Cancelling stream {cid} to "
                    f"{self._portal.channel.uid}")

                # NOTE: we're telling the far end actor to cancel a task
                # corresponding to *this actor*. The far end local channel
                # instance is passed to `Actor._cancel_task()` implicitly.
                await self._portal.run_from_ns('self', '_cancel_task', cid=cid)

            if cs.cancelled_caught:
                # XXX: there's no way to know if the remote task was indeed
                # cancelled in the case where the connection is broken or
                # some other network error occurred.
                # if not self._portal.channel.connected():
                if not self.chan.connected():
                    log.warning(
                        "May have failed to cancel remote task "
                        f"{cid} for {self._portal.channel.uid}")
        else:
            # ensure callee side
            assert self._cancel_scope
            # TODO: should we have an explicit cancel message
            # or is relaying the local `trio.Cancelled` as an
            # {'error': trio.Cancelled, cid: "blah"} enough?
            # This probably gets into the discussion in
            # https://github.com/goodboy/tractor/issues/36
            self._cancel_scope.cancel()

    # TODO: do we need a restart api?
    # async def restart(self) -> None:
    #     pass

    @asynccontextmanager
    async def open_stream(
        self,
        shield: bool = False,
    ) -> AsyncGenerator[MsgStream, None]:
        # TODO

        actor = current_actor()

        # here we create a mem chan that corresponds to the
        # far end caller / callee.

        # NOTE: in one way streaming this only happens on the
        # caller side inside `Actor.send_cmd()` so if you try
        # to send a stop from the caller to the callee in the
        # single-direction-stream case you'll get a lookup error
        # currently.
        _, recv_chan = actor.get_memchans(
            self.chan.uid,
            self.cid
        )

        async with MsgStream(
            ctx=self,
            rx_chan=recv_chan,
            shield=shield,
        ) as rchan:

            if self._portal:
                self._portal._streams.add(rchan)

            try:
                yield rchan

            finally:
                # signal ``StopAsyncIteration`` on far end.
                await self.send_stop()

                if self._portal:
                    self._portal._streams.remove(rchan)

    async def started(self, value: Any) -> None:

        if self._portal:
            raise RuntimeError(
                f"Caller side context {self} can not call started!")

        await self.chan.send({'started': value, 'cid': self.cid})


def stream(func: Callable) -> Callable:
    """Mark an async function as a streaming routine with ``@stream``.

    """
    # annotate
    # TODO: apply whatever solution ``mypy`` ends up picking for this:
    # https://github.com/python/mypy/issues/2087#issuecomment-769266912
    func._tractor_stream_function = True  # type: ignore

    sig = inspect.signature(func)
    params = sig.parameters
    if 'stream' not in params and 'ctx' in params:
        warnings.warn(
            "`@tractor.stream decorated funcs should now declare a `stream` "
            " arg, `ctx` is now designated for use with @tractor.context",
            DeprecationWarning,
            stacklevel=2,
        )

    if (
        'ctx' not in params and
        'to_trio' not in params and
        'stream' not in params
    ):
        raise TypeError(
            "The first argument to the stream function "
            f"{func.__name__} must be `ctx: tractor.Context` "
            "(Or ``to_trio`` if using ``asyncio`` in guest mode)."
        )
    return func


def context(func: Callable) -> Callable:
    """Mark an async function as a streaming routine with ``@context``.

    """
    # annotate
    # TODO: apply whatever solution ``mypy`` ends up picking for this:
    # https://github.com/python/mypy/issues/2087#issuecomment-769266912
    func._tractor_context_function = True  # type: ignore

    sig = inspect.signature(func)
    params = sig.parameters
    if 'ctx' not in params:
        raise TypeError(
            "The first argument to the context function "
            f"{func.__name__} must be `ctx: tractor.Context`"
        )
    return func

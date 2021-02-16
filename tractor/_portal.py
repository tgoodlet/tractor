"""
Portal api
"""
import importlib
import inspect
import typing
from typing import Tuple, Any, Dict, Optional, Set, Iterator
from functools import partial
from dataclasses import dataclass
from contextlib import contextmanager
import warnings

import trio
from async_generator import asynccontextmanager

from ._state import current_actor
from ._ipc import Channel
from .log import get_logger
from ._exceptions import unpack_error, NoResult, RemoteActorError


log = get_logger('tractor')


@asynccontextmanager
async def maybe_open_nursery(
    nursery: trio.Nursery = None,
    shield: bool = False,
) -> typing.AsyncGenerator[trio.Nursery, Any]:
    """Create a new nursery if None provided.

    Blocks on exit as expected if no input nursery is provided.
    """
    if nursery is not None:
        yield nursery
    else:
        async with trio.open_nursery() as nursery:
            nursery.cancel_scope.shield = shield
            yield nursery


class ReceiveStream(trio.abc.ReceiveChannel):
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
        cid: str,
        rx_chan: trio.abc.ReceiveChannel,
        portal: 'Portal',
    ) -> None:
        self._cid = cid
        self._rx_chan = rx_chan
        self._portal = portal
        self._shielded = False

    # delegate directly to underlying mem channel
    def receive_nowait(self):
        return self._rx_chan.receive_nowait()

    async def receive(self):
        try:
            msg = await self._rx_chan.receive()
            return msg['yield']

        except trio.ClosedResourceError:
            # when the send is closed we assume the stream has
            # terminated and signal this local iterator to stop
            await self.aclose()
            raise StopAsyncIteration

        except trio.Cancelled:
            # relay cancels to the remote task
            await self.aclose()
            raise

        except KeyError:
            # internal error should never get here
            assert msg.get('cid'), (
                "Received internal error at portal?")
            raise unpack_error(msg, self._portal.channel)

    @contextmanager
    def shield(
        self
    ) -> Iterator['ReceiveStream']:  # noqa
        """Shield this stream's underlying channel such that a local consumer task
        can be cancelled (and possibly restarted) using ``trio.Cancelled``.

        """
        self._shielded = True
        yield self
        self._shielded = False

    async def aclose(self):
        """Cancel associated remote actor task and local memory channel
        on close.
        """
        rx_chan = self._rx_chan
        stats = rx_chan.statistics()

        if rx_chan._closed:
            log.warning(f"{self} is already closed")
            return

        if stats.open_receive_channels > 1:
            # if we've been cloned don't kill the stream
            log.debug("there are still consumers running keeping stream alive")
            return

        if self._shielded:
            log.warning(f"{self} is shielded, portal channel being kept alive")
            return

        # close the local mem chan
        rx_chan.close()

        cid = self._cid
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
            if not self._portal.channel.connected():
                log.warning(
                    "May have failed to cancel remote task "
                    f"{cid} for {self._portal.channel.uid}")

    def clone(self):
        """Clone this receive channel allowing for multi-task
        consumption from the same channel.

        """
        return ReceiveStream(
            self._cid,
            self._rx_chan.clone(),
            self._portal,
        )


class Portal:
    """A 'portal' to a(n) (remote) ``Actor``.

    Allows for invoking remote routines and receiving results through an
    underlying ``tractor.Channel`` as though the remote (async)
    function / generator was invoked locally.

    Think of this like a native async IPC API.
    """
    def __init__(self, channel: Channel) -> None:
        self.channel = channel
        # when this is set to a tuple returned from ``_submit()`` then
        # it is expected that ``result()`` will be awaited at some point
        # during the portal's lifetime
        self._result: Optional[Any] = None
        # set when _submit_for_result is called
        self._expect_result: Optional[
            Tuple[str, Any, str, Dict[str, Any]]
        ] = None
        self._streams: Set[ReceiveStream] = set()
        self.actor = current_actor()

    async def _submit(
        self,
        ns: str,
        func: str,
        kwargs,
    ) -> Tuple[str, trio.abc.ReceiveChannel, str, Dict[str, Any]]:
        """Submit a function to be scheduled and run by actor, return the
        associated caller id, response queue, response type str,
        first message packet as a tuple.

        This is an async call.
        """
        # ship a function call request to the remote actor
        cid, recv_chan = await self.actor.send_cmd(
            self.channel, ns, func, kwargs)

        # wait on first response msg and handle (this should be
        # in an immediate response)

        first_msg = await recv_chan.receive()
        functype = first_msg.get('functype')

        if functype == 'function' or functype == 'asyncfunction':
            resp_type = 'return'
        elif functype == 'asyncgen':
            resp_type = 'yield'
        elif 'error' in first_msg:
            raise unpack_error(first_msg, self.channel)
        else:
            raise ValueError(f"{first_msg} is an invalid response packet?")

        return cid, recv_chan, resp_type, first_msg

    async def _submit_for_result(self, ns: str, func: str, **kwargs) -> None:
        assert self._expect_result is None, \
                "A pending main result has already been submitted"
        self._expect_result = await self._submit(ns, func, kwargs)

    async def run(
        self,
        func_or_ns: str,
        fn_name: Optional[str] = None,
        **kwargs
    ) -> Any:
        """Submit a remote function to be scheduled and run by actor, in
        a new task, wrap and return its (stream of) result(s).

        This is a blocking call and returns either a value from the
        remote rpc task or a local async generator instance.
        """
        if isinstance(func_or_ns, str):
            warnings.warn(
                "`Portal.run(namespace: str, funcname: str)` is now"
                "deprecated, pass a function reference directly instead\n"
                "If you still want to run a remote function by name use"
                "`Portal.run_from_ns()`",
                DeprecationWarning,
                stacklevel=2,
            )
            fn_mod_path = func_or_ns
            assert isinstance(fn_name, str)

        else:  # function reference was passed directly
            fn = func_or_ns
            fn_mod_path = fn.__module__
            fn_name = fn.__name__

        return await self._return_from_resptype(
            *(await self._submit(fn_mod_path, fn_name, kwargs))
        )

    async def run_from_ns(
        self,
        namespace_path: str,
        function_name: str,
        **kwargs,
    ) -> Any:
        """Run a function from a (remote) namespace in a new task on the far-end actor.

        This is a more explitcit way to run tasks in a remote-process
        actor using explicit object-path syntax. Hint: this is how
        `.run()` works underneath.

        Note::

            A special namespace `self` can be used to invoke `Actor`
            instance methods in the remote runtime. Currently this should only
            be used for `tractor` internals.
        """
        return await self._return_from_resptype(
            *(await self._submit(namespace_path, function_name, kwargs))
        )

    async def _return_from_resptype(
        self,
        cid: str,
        recv_chan: trio.abc.ReceiveChannel,
        resptype: str,
        first_msg: dict
    ) -> Any:
        # TODO: not this needs some serious work and thinking about how
        # to make async-generators the fundamental IPC API over channels!
        # (think `yield from`, `gen.send()`, and functional reactive stuff)
        if resptype == 'yield':  # stream response
            rchan = ReceiveStream(cid, recv_chan, self)
            self._streams.add(rchan)
            return rchan

        elif resptype == 'return':  # single response
            msg = await recv_chan.receive()
            try:
                return msg['return']
            except KeyError:
                # internal error should never get here
                assert msg.get('cid'), "Received internal error at portal?"
                raise unpack_error(msg, self.channel)
        else:
            raise ValueError(f"Unknown msg response type: {first_msg}")

    async def result(self) -> Any:
        """Return the result(s) from the remote actor's "main" task.
        """
        # Check for non-rpc errors slapped on the
        # channel for which we always raise
        exc = self.channel._exc
        if exc:
            raise exc

        # not expecting a "main" result
        if self._expect_result is None:
            log.warning(
                f"Portal for {self.channel.uid} not expecting a final"
                " result?\nresult() should only be called if subactor"
                " was spawned with `ActorNursery.run_in_actor()`")
            return NoResult

        # expecting a "main" result
        assert self._expect_result
        if self._result is None:
            try:
                self._result = await self._return_from_resptype(
                    *self._expect_result
                )
            except RemoteActorError as err:
                self._result = err

        # re-raise error on every call
        if isinstance(self._result, RemoteActorError):
            raise self._result

        return self._result

    async def _cancel_streams(self):
        # terminate all locally running async generator
        # IPC calls
        if self._streams:
            log.warning(
                f"Cancelling all streams with {self.channel.uid}")
            for stream in self._streams.copy():
                try:
                    await stream.aclose()
                except trio.ClosedResourceError:
                    # don't error the stream having already been closed
                    # (unless of course at some point down the road we
                    # won't expect this to always be the case or need to
                    # detect it for respawning purposes?)
                    log.debug(f"{stream} was already closed.")

    async def aclose(self):
        log.debug(f"Closing {self}")
        # TODO: once we move to implementing our own `ReceiveChannel`
        # (including remote task cancellation inside its `.aclose()`)
        # we'll need to .aclose all those channels here
        await self._cancel_streams()

    async def cancel_actor(self):
        """Cancel the actor on the other end of this portal.
        """
        if not self.channel.connected():
            log.warning("This portal is already closed can't cancel")
            return False

        await self._cancel_streams()

        log.warning(
            f"Sending actor cancel request to {self.channel.uid} on "
            f"{self.channel}")
        try:
            # send cancel cmd - might not get response
            # XXX: sure would be nice to make this work with a proper shield
            # with trio.CancelScope() as cancel_scope:
            # with trio.CancelScope(shield=True) as cancel_scope:
            with trio.move_on_after(0.5) as cancel_scope:
                cancel_scope.shield = True

                await self.run_from_ns('self', 'cancel')
                return True

            if cancel_scope.cancelled_caught:
                log.warning(f"May have failed to cancel {self.channel.uid}")

            # if we get here some weird cancellation case happened
            return False

        except trio.ClosedResourceError:
            log.warning(
                f"{self.channel} for {self.channel.uid} was already closed?")
            return False


@dataclass
class LocalPortal:
    """A 'portal' to a local ``Actor``.

    A compatibility shim for normal portals but for invoking functions
    using an in process actor instance.
    """
    actor: 'Actor'  # type: ignore # noqa
    channel: Channel

    async def run_from_ns(self, ns: str, func_name: str, **kwargs) -> Any:
        """Run a requested local function from a namespace path and
        return it's result.

        """
        obj = self.actor if ns == 'self' else importlib.import_module(ns)
        func = getattr(obj, func_name)
        if inspect.iscoroutinefunction(func):
            return await func(**kwargs)
        else:
            return func(**kwargs)


@asynccontextmanager
async def open_portal(
    channel: Channel,
    nursery: Optional[trio.Nursery] = None,
    start_msg_loop: bool = True,
    shield: bool = False,
) -> typing.AsyncGenerator[Portal, None]:
    """Open a ``Portal`` through the provided ``channel``.

    Spawns a background task to handle message processing.
    """
    actor = current_actor()
    assert actor
    was_connected = False

    async with maybe_open_nursery(nursery, shield=shield) as nursery:
        if not channel.connected():
            await channel.connect()
            was_connected = True

        if channel.uid is None:
            await actor._do_handshake(channel)

        msg_loop_cs: Optional[trio.CancelScope] = None
        if start_msg_loop:
            msg_loop_cs = await nursery.start(
                partial(
                    actor._process_messages,
                    channel,
                    # if the local task is cancelled we want to keep
                    # the msg loop running until our block ends
                    shield=True,
                )
            )
        portal = Portal(channel)
        try:
            yield portal
        finally:
            await portal.aclose()

            if was_connected:
                # cancel remote channel-msg loop
                await channel.send(None)

            # cancel background msg loop task
            if msg_loop_cs:
                msg_loop_cs.cancel()

            nursery.cancel_scope.cancel()

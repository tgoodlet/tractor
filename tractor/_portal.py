"""
Portal api
"""
import importlib
import inspect
from typing import (
    Tuple, Any, Dict, Optional, Set,
    Callable, AsyncGenerator
)
from functools import partial
from dataclasses import dataclass
import warnings

import trio
from async_generator import asynccontextmanager

from ._state import current_actor
from ._ipc import Channel
from .log import get_logger
from ._exceptions import unpack_error, NoResult, RemoteActorError
from ._streaming import Context, ReceiveMsgStream


log = get_logger(__name__)


@asynccontextmanager
async def maybe_open_nursery(
    nursery: trio.Nursery = None,
    shield: bool = False,
) -> AsyncGenerator[trio.Nursery, Any]:
    """Create a new nursery if None provided.

    Blocks on exit as expected if no input nursery is provided.
    """
    if nursery is not None:
        yield nursery
    else:
        async with trio.open_nursery() as nursery:
            nursery.cancel_scope.shield = shield
            yield nursery


def func_deats(func: Callable) -> Tuple[str, str]:
    return (
        func.__module__,
        func.__name__,
    )


class Portal:
    """A 'portal' to a(n) (remote) ``Actor``.

    A portal is "opened" (and eventually closed) by one side of an
    inter-actor communication context. The side which opens the portal
    is equivalent to a "caller" in function parlance and usually is
    either the called actor's parent (in process tree hierarchy terms)
    or a client interested in scheduling work to be done remotely in a
    far process.

    The portal api allows the "caller" actor to invoke remote routines
    and receive results through an underlying ``tractor.Channel`` as
    though the remote (async) function / generator was called locally.
    It may be thought of loosely as an RPC api where native Python
    function calling semantics are supported transparently; hence it is
    like having a "portal" between the seperate actor memory spaces.

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
        self._streams: Set[ReceiveMsgStream] = set()
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

        if 'error' in first_msg:
            raise unpack_error(first_msg, self.channel)

        elif functype not in ('asyncfunc', 'asyncgen', 'context'):
            raise ValueError(f"{first_msg} is an invalid response packet?")

        return cid, recv_chan, functype, first_msg

    async def _submit_for_result(self, ns: str, func: str, **kwargs) -> None:

        assert self._expect_result is None, \
                "A pending main result has already been submitted"

        self._expect_result = await self._submit(ns, func, kwargs)

    async def _return_once(
        self,
        cid: str,
        recv_chan: trio.abc.ReceiveChannel,
        resptype: str,
        first_msg: dict
    ) -> Any:
        assert resptype == 'asyncfunc'  # single response

        msg = await recv_chan.receive()
        try:
            return msg['return']
        except KeyError:
            # internal error should never get here
            assert msg.get('cid'), "Received internal error at portal?"
            raise unpack_error(msg, self.channel)

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
                self._result = await self._return_once(*self._expect_result)
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
        return await self._return_once(
            *(await self._submit(namespace_path, function_name, kwargs))
        )

    async def run(
        self,
        func: str,
        fn_name: Optional[str] = None,
        **kwargs
    ) -> Any:
        """Submit a remote function to be scheduled and run by actor, in
        a new task, wrap and return its (stream of) result(s).

        This is a blocking call and returns either a value from the
        remote rpc task or a local async generator instance.
        """
        if isinstance(func, str):
            warnings.warn(
                "`Portal.run(namespace: str, funcname: str)` is now"
                "deprecated, pass a function reference directly instead\n"
                "If you still want to run a remote function by name use"
                "`Portal.run_from_ns()`",
                DeprecationWarning,
                stacklevel=2,
            )
            fn_mod_path = func
            assert isinstance(fn_name, str)

        else:  # function reference was passed directly
            if (
                not inspect.iscoroutinefunction(func) or
                (
                    inspect.iscoroutinefunction(func) and
                    getattr(func, '_tractor_stream_function', False)
                )
            ):
                raise TypeError(
                    f'{func} must be a non-streaming async function!')

            fn_mod_path, fn_name = func_deats(func)

        return await self._return_once(
            *(await self._submit(fn_mod_path, fn_name, kwargs))
        )

    @asynccontextmanager
    async def open_stream_from(
        self,
        async_gen_func: Callable,  # typing: ignore
        **kwargs,
    ) -> AsyncGenerator[ReceiveMsgStream, None]:

        if not inspect.isasyncgenfunction(async_gen_func):
            if not (
                inspect.iscoroutinefunction(async_gen_func) and
                getattr(async_gen_func, '_tractor_stream_function', False)
            ):
                raise TypeError(
                    f'{async_gen_func} must be an async generator function!')

        fn_mod_path, fn_name = func_deats(async_gen_func)
        (
            cid,
            recv_chan,
            functype,
            first_msg
        ) = await self._submit(fn_mod_path, fn_name, kwargs)

        # receive only stream
        assert functype == 'asyncgen'

        ctx = Context(self.channel, cid, _portal=self)
        try:
            # deliver receive only stream
            async with ReceiveMsgStream(ctx, recv_chan) as rchan:
                self._streams.add(rchan)
                yield rchan

        finally:

            # cancel the far end task on consumer close
            # NOTE: this is a special case since we assume that if using
            # this ``.open_fream_from()`` api, the stream is one a one
            # time use and we couple the far end tasks's lifetime to
            # the consumer's scope; we don't ever send a `'stop'`
            # message right now since there shouldn't be a reason to
            # stop and restart the stream, right?
            try:
                await ctx.cancel()
            except trio.ClosedResourceError:
                # if the far end terminates before we send a cancel the
                # underlying transport-channel may already be closed.
                log.debug(f'Context {ctx} was already closed?')

            self._streams.remove(rchan)

    @asynccontextmanager
    async def open_context(
        self,
        func: Callable,
        **kwargs,
    ) -> Context:
        """Open an inter-actor task context.

        This is a synchronous API which allows for deterministic
        setup/teardown of a remote task. The yielded ``Context`` further
        allows for opening bidirectional streams - see
        ``Context.open_stream()``.

        """
        # conduct target func method structural checks
        if not inspect.iscoroutinefunction(func) and (
            getattr(func, '_tractor_contex_function', False)
        ):
            raise TypeError(
                f'{func} must be an async generator function!')

        fn_mod_path, fn_name = func_deats(func)

        cid, recv_chan, functype, first_msg = await self._submit(
            fn_mod_path, fn_name, kwargs)

        assert functype == 'context'

        msg = await recv_chan.receive()
        try:
            # the "first" value here is delivered by the callee's
            # ``Context.started()`` call.
            first = msg['started']

        except KeyError:
            assert msg.get('cid'), ("Received internal error at context?")

            if msg.get('error'):
                # raise the error message
                raise unpack_error(msg, self.channel)
            else:
                raise
        try:
            ctx = Context(self.channel, cid, _portal=self)
            yield ctx, first

        finally:
            await recv_chan.aclose()
            await ctx.cancel()


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
        return await func(**kwargs)


@asynccontextmanager
async def open_portal(
    channel: Channel,
    nursery: Optional[trio.Nursery] = None,
    start_msg_loop: bool = True,
    shield: bool = False,
) -> AsyncGenerator[Portal, None]:
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

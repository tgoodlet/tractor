"""
Actor primitives and helpers
"""
from collections import defaultdict
from functools import partial
from itertools import chain
import importlib
import importlib.util
import inspect
import uuid
import typing
from typing import Dict, List, Tuple, Any, Optional, Union
from types import ModuleType
import sys
import os
from contextlib import ExitStack

import trio  # type: ignore
from trio_typing import TaskStatus
from async_generator import aclosing

from ._ipc import Channel
from ._streaming import Context, _context
from .log import get_logger
from ._exceptions import (
    pack_error,
    unpack_error,
    ModuleNotExposed,
    is_multi_cancelled,
)
from . import _debug
from ._discovery import get_arbiter
from ._portal import Portal
from . import _state
from . import _mp_fixup_main


log = get_logger('tractor')


class ActorFailure(Exception):
    "General actor failure"


async def _invoke(
    actor: 'Actor',
    cid: str,
    chan: Channel,
    func: typing.Callable,
    kwargs: Dict[str, Any],
    task_status: TaskStatus[
        Union[trio.CancelScope, BaseException]
    ] = trio.TASK_STATUS_IGNORED,
):
    """Invoke local func and deliver result(s) over provided channel.
    """
    treat_as_gen = False
    cs = None
    cancel_scope = trio.CancelScope()
    ctx = Context(chan, cid, cancel_scope)
    _context.set(ctx)
    if getattr(func, '_tractor_stream_function', False):
        # handle decorated ``@tractor.stream`` async functions
        kwargs['ctx'] = ctx
        treat_as_gen = True
    try:
        is_async_partial = False
        is_async_gen_partial = False
        if isinstance(func, partial):
            is_async_partial = inspect.iscoroutinefunction(func.func)
            is_async_gen_partial = inspect.isasyncgenfunction(func.func)

        if (
            not inspect.iscoroutinefunction(func) and
            not inspect.isasyncgenfunction(func) and
            not is_async_partial and
            not is_async_gen_partial
        ):
            await chan.send({'functype': 'function', 'cid': cid})
            with cancel_scope as cs:
                task_status.started(cs)
                await chan.send({'return': func(**kwargs), 'cid': cid})
        else:
            coro = func(**kwargs)

            if inspect.isasyncgen(coro):
                await chan.send({'functype': 'asyncgen', 'cid': cid})
                # XXX: massive gotcha! If the containing scope
                # is cancelled and we execute the below line,
                # any ``ActorNursery.__aexit__()`` WON'T be
                # triggered in the underlying async gen! So we
                # have to properly handle the closing (aclosing)
                # of the async gen in order to be sure the cancel
                # is propagated!
                with cancel_scope as cs:
                    task_status.started(cs)
                    async with aclosing(coro) as agen:
                        async for item in agen:
                            # TODO: can we send values back in here?
                            # it's gonna require a `while True:` and
                            # some non-blocking way to retrieve new `asend()`
                            # values from the channel:
                            # to_send = await chan.recv_nowait()
                            # if to_send is not None:
                            #     to_yield = await coro.asend(to_send)
                            await chan.send({'yield': item, 'cid': cid})

                log.debug(f"Finished iterating {coro}")
                # TODO: we should really support a proper
                # `StopAsyncIteration` system here for returning a final
                # value if desired
                await chan.send({'stop': True, 'cid': cid})
            else:
                if treat_as_gen:
                    await chan.send({'functype': 'asyncgen', 'cid': cid})
                    # XXX: the async-func may spawn further tasks which push
                    # back values like an async-generator would but must
                    # manualy construct the response dict-packet-responses as
                    # above
                    with cancel_scope as cs:
                        task_status.started(cs)
                        await coro
                    if not cs.cancelled_caught:
                        # task was not cancelled so we can instruct the
                        # far end async gen to tear down
                        await chan.send({'stop': True, 'cid': cid})
                else:
                    await chan.send({'functype': 'asyncfunction', 'cid': cid})
                    with cancel_scope as cs:
                        task_status.started(cs)
                        await chan.send({'return': await coro, 'cid': cid})

    except (Exception, trio.MultiError) as err:

        # TODO: maybe we'll want differnet "levels" of debugging
        # eventualy such as ('app', 'supervisory', 'runtime') ?
        if not isinstance(err, trio.ClosedResourceError) and (
            not is_multi_cancelled(err)
        ):
            # XXX: is there any case where we'll want to debug IPC
            # disconnects? I can't think of a reason that inspecting
            # this type of failure will be useful for respawns or
            # recovery logic - the only case is some kind of strange bug
            # in `trio` itself?
            entered = await _debug._maybe_enter_pm(err)
            if not entered:
                log.exception("Actor crashed:")

        # always ship errors back to caller
        err_msg = pack_error(err)
        err_msg['cid'] = cid
        try:
            await chan.send(err_msg)
        except trio.ClosedResourceError:
            log.warning(
                f"Failed to ship error to caller @ {chan.uid}")
        if cs is None:
            # error is from above code not from rpc invocation
            task_status.started(err)

    finally:
        # RPC task bookeeping
        try:
            scope, func, is_complete = actor._rpc_tasks.pop((chan, cid))
            is_complete.set()
        except KeyError:
            # If we're cancelled before the task returns then the
            # cancel scope will not have been inserted yet
            log.warning(
                f"Task {func} likely errored or cancelled before it started")
        finally:
            if not actor._rpc_tasks:
                log.info("All RPC tasks have completed")
                actor._ongoing_rpc_tasks.set()


def _get_mod_abspath(module):
    return os.path.abspath(module.__file__)


# process-global stack closed at end on actor runtime teardown
_lifetime_stack: ExitStack = ExitStack()


class Actor:
    """The fundamental concurrency primitive.

    An *actor* is the combination of a regular Python or
    ``multiprocessing.Process`` executing a ``trio`` task tree, communicating
    with other actors through "portals" which provide a native async API
    around "channels".
    """
    is_arbiter: bool = False

    # nursery placeholders filled in by `_async_main()` after fork
    _root_n: Optional[trio.Nursery] = None
    _service_n: Optional[trio.Nursery] = None
    _server_n: Optional[trio.Nursery] = None

    # Information about `__main__` from parent
    _parent_main_data: Dict[str, str]
    _parent_chan_cs: Optional[trio.CancelScope] = None

    def __init__(
        self,
        name: str,
        *,
        enable_modules: List[str] = [],
        uid: str = None,
        loglevel: str = None,
        arbiter_addr: Optional[Tuple[str, int]] = None,
        spawn_method: Optional[str] = None
    ) -> None:
        """This constructor is called in the parent actor **before** the spawning
        phase (aka before a new process is executed).
        """
        self.name = name
        self.uid = (name, uid or str(uuid.uuid4()))

        self._cancel_complete = trio.Event()
        self._cancel_called: bool = False

        # retreive and store parent `__main__` data which
        # will be passed to children
        self._parent_main_data = _mp_fixup_main._mp_figure_out_main()

        # always include debugging tools module
        enable_modules.append('tractor._debug')

        mods = {}
        for name in enable_modules:
            mod = importlib.import_module(name)
            mods[name] = _get_mod_abspath(mod)

        self.enable_modules = mods
        self._mods: Dict[str, ModuleType] = {}

        # TODO: consider making this a dynamically defined
        # @dataclass once we get py3.7
        self.loglevel = loglevel
        self._arb_addr = arbiter_addr

        # marked by the process spawning backend at startup
        # will be None for the parent most process started manually
        # by the user (currently called the "arbiter")
        self._spawn_method = spawn_method

        self._peers: defaultdict = defaultdict(list)
        self._peer_connected: dict = {}
        self._no_more_peers = trio.Event()
        self._no_more_peers.set()
        self._ongoing_rpc_tasks = trio.Event()
        self._ongoing_rpc_tasks.set()
        # (chan, cid) -> (cancel_scope, func)
        self._rpc_tasks: Dict[
            Tuple[Channel, str],
            Tuple[trio.CancelScope, typing.Callable, trio.Event]
        ] = {}
        # map {uids -> {callids -> waiter queues}}
        self._cids2qs: Dict[
            Tuple[Tuple[str, str], str],
            Tuple[
                trio.abc.SendChannel[Any],
                trio.abc.ReceiveChannel[Any]
            ]
        ] = {}
        self._listeners: List[trio.abc.Listener] = []
        self._parent_chan: Optional[Channel] = None
        self._forkserver_info: Optional[
            Tuple[Any, Any, Any, Any, Any]] = None
        self._actoruid2nursery: Dict[str, 'ActorNursery'] = {}  # type: ignore

    async def wait_for_peer(
        self, uid: Tuple[str, str]
    ) -> Tuple[trio.Event, Channel]:
        """Wait for a connection back from a spawned actor with a given
        ``uid``.
        """
        log.debug(f"Waiting for peer {uid} to connect")
        event = self._peer_connected.setdefault(uid, trio.Event())
        await event.wait()
        log.debug(f"{uid} successfully connected back to us")
        return event, self._peers[uid][-1]

    def load_modules(self) -> None:
        """Load allowed RPC modules locally (after fork).

        Since this actor may be spawned on a different machine from
        the original nursery we need to try and load the local module
        code (if it exists).
        """
        try:
            if self._spawn_method == 'trio':
                parent_data = self._parent_main_data
                if 'init_main_from_name' in parent_data:
                    _mp_fixup_main._fixup_main_from_name(
                        parent_data['init_main_from_name'])
                elif 'init_main_from_path' in parent_data:
                    _mp_fixup_main._fixup_main_from_path(
                        parent_data['init_main_from_path'])

            for modpath, filepath in self.enable_modules.items():
                # XXX append the allowed module to the python path which
                # should allow for relative (at least downward) imports.
                sys.path.append(os.path.dirname(filepath))
                log.debug(f"Attempting to import {modpath}@{filepath}")
                mod = importlib.import_module(modpath)
                self._mods[modpath] = mod
                if modpath == '__main__':
                    self._mods['__mp_main__'] = mod
        except ModuleNotFoundError:
            # it is expected the corresponding `ModuleNotExposed` error
            # will be raised later
            log.error(f"Failed to import {modpath} in {self.name}")
            raise

    def _get_rpc_func(self, ns, funcname):
        try:
            return getattr(self._mods[ns], funcname)
        except KeyError as err:
            mne = ModuleNotExposed(*err.args)

            if ns == '__main__':
                msg = (
                    "\n\nMake sure you exposed the current module using:\n\n"
                    "ActorNursery.start_actor(<name>, enable_modules="
                    "[__name__])"
                )

                mne.msg += msg

            raise mne

    async def _stream_handler(
        self,
        stream: trio.SocketStream,
    ) -> None:
        """Entry point for new inbound connections to the channel server.
        """
        self._no_more_peers = trio.Event()  # unset
        chan = Channel(stream=stream)
        log.info(f"New connection to us {chan}")

        # send/receive initial handshake response
        try:
            uid = await self._do_handshake(chan)
        except StopAsyncIteration:
            log.warning(f"Channel {chan} failed to handshake")
            return

        # channel tracking
        event = self._peer_connected.pop(uid, None)
        if event:
            # Instructing connection: this is likely a new channel to
            # a recently spawned actor which we'd like to control via
            # async-rpc calls.
            log.debug(f"Waking channel waiters {event.statistics()}")
            # Alert any task waiting on this connection to come up
            event.set()

        chans = self._peers[uid]
        if chans:
            log.warning(
                f"already have channel(s) for {uid}:{chans}?"
            )
        log.trace(f"Registered {chan} for {uid}")  # type: ignore
        # append new channel
        self._peers[uid].append(chan)

        # Begin channel management - respond to remote requests and
        # process received reponses.
        try:
            await self._process_messages(chan)
        finally:
            # Drop ref to channel so it can be gc-ed and disconnected
            log.debug(f"Releasing channel {chan} from {chan.uid}")
            chans = self._peers.get(chan.uid)
            chans.remove(chan)
            if not chans:
                log.debug(f"No more channels for {chan.uid}")
                self._peers.pop(chan.uid, None)

            log.debug(f"Peers is {self._peers}")

            if not self._peers:  # no more channels connected
                self._no_more_peers.set()
                log.debug("Signalling no more peer channels")

            # # XXX: is this necessary (GC should do it?)
            if chan.connected():
                log.debug(f"Disconnecting channel {chan}")
                try:
                    # send our msg loop terminate sentinel
                    await chan.send(None)
                    # await chan.aclose()
                except trio.BrokenResourceError:
                    log.exception(
                        f"Channel for {chan.uid} was already zonked..")

    async def _push_result(
        self,
        chan: Channel,
        msg: Dict[str, Any],
    ) -> None:
        """Push an RPC result to the local consumer's queue.
        """
        actorid = chan.uid
        assert actorid, f"`actorid` can't be {actorid}"
        cid = msg['cid']
        send_chan, recv_chan = self._cids2qs[(actorid, cid)]
        assert send_chan.cid == cid  # type: ignore
        if 'stop' in msg:
            log.debug(f"{send_chan} was terminated at remote end")
            return await send_chan.aclose()
        try:
            log.debug(f"Delivering {msg} from {actorid} to caller {cid}")
            # maintain backpressure
            await send_chan.send(msg)
        except trio.BrokenResourceError:
            # XXX: local consumer has closed their side
            # so cancel the far end streaming task
            log.warning(f"{send_chan} consumer is already closed")

    def get_memchans(
        self,
        actorid: Tuple[str, str],
        cid: str
    ) -> Tuple[trio.abc.SendChannel, trio.abc.ReceiveChannel]:
        log.debug(f"Getting result queue for {actorid} cid {cid}")
        try:
            send_chan, recv_chan = self._cids2qs[(actorid, cid)]
        except KeyError:
            send_chan, recv_chan = trio.open_memory_channel(1000)
            send_chan.cid = cid  # type: ignore
            recv_chan.cid = cid  # type: ignore
            self._cids2qs[(actorid, cid)] = send_chan, recv_chan

        return send_chan, recv_chan

    async def send_cmd(
        self,
        chan: Channel,
        ns: str,
        func: str,
        kwargs: dict
    ) -> Tuple[str, trio.abc.ReceiveChannel]:
        """Send a ``'cmd'`` message to a remote actor and return a
        caller id and a ``trio.Queue`` that can be used to wait for
        responses delivered by the local message processing loop.
        """
        cid = str(uuid.uuid4())
        assert chan.uid
        send_chan, recv_chan = self.get_memchans(chan.uid, cid)
        log.debug(f"Sending cmd to {chan.uid}: {ns}.{func}({kwargs})")
        await chan.send({'cmd': (ns, func, kwargs, self.uid, cid)})
        return cid, recv_chan

    async def _process_messages(
        self,
        chan: Channel,
        shield: bool = False,
        task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
    ) -> None:
        """Process messages for the channel async-RPC style.

        Receive multiplexed RPC requests and deliver responses over ``chan``.
        """
        # TODO: once https://github.com/python-trio/trio/issues/467 gets
        # worked out we'll likely want to use that!
        msg = None
        log.debug(f"Entering msg loop for {chan} from {chan.uid}")
        try:
            with trio.CancelScope(shield=shield) as loop_cs:
                # this internal scope allows for keeping this message
                # loop running despite the current task having been
                # cancelled (eg. `open_portal()` may call this method from
                # a locally spawned task) and recieve this scope using
                # ``scope = Nursery.start()``
                task_status.started(loop_cs)
                async for msg in chan:
                    if msg is None:  # loop terminate sentinel
                        log.debug(
                            f"Cancelling all tasks for {chan} from {chan.uid}")
                        for (channel, cid) in self._rpc_tasks:
                            if channel is chan:
                                await self._cancel_task(cid, channel)
                        log.debug(
                                f"Msg loop signalled to terminate for"
                                f" {chan} from {chan.uid}")
                        break

                    log.trace(   # type: ignore
                        f"Received msg {msg} from {chan.uid}")
                    if msg.get('cid'):
                        # deliver response to local caller/waiter
                        await self._push_result(chan, msg)
                        log.debug(
                            f"Waiting on next msg for {chan} from {chan.uid}")
                        continue

                    # process command request
                    try:
                        ns, funcname, kwargs, actorid, cid = msg['cmd']
                    except KeyError:
                        # This is the non-rpc error case, that is, an
                        # error **not** raised inside a call to ``_invoke()``
                        # (i.e. no cid was provided in the msg - see above).
                        # Push this error to all local channel consumers
                        # (normally portals) by marking the channel as errored
                        assert chan.uid
                        exc = unpack_error(msg, chan=chan)
                        chan._exc = exc
                        raise exc

                    log.debug(
                        f"Processing request from {actorid}\n"
                        f"{ns}.{funcname}({kwargs})")
                    if ns == 'self':
                        func = getattr(self, funcname)
                        if funcname == '_cancel_task':
                            # XXX: a special case is made here for
                            # remote calls since we don't want the
                            # remote actor have to know which channel
                            # the task is associated with and we can't
                            # pass non-primitive types between actors.
                            # This means you can use:
                            #    Portal.run('self', '_cancel_task, cid=did)
                            # without passing the `chan` arg.
                            kwargs['chan'] = chan
                    else:
                        # complain to client about restricted modules
                        try:
                            func = self._get_rpc_func(ns, funcname)
                        except (ModuleNotExposed, AttributeError) as err:
                            err_msg = pack_error(err)
                            err_msg['cid'] = cid
                            await chan.send(err_msg)
                            continue

                    # spin up a task for the requested function
                    log.debug(f"Spawning task for {func}")
                    assert self._service_n
                    cs = await self._service_n.start(
                        partial(_invoke, self, cid, chan, func, kwargs),
                        name=funcname,
                    )
                    # never allow cancelling cancel requests (results in
                    # deadlock and other weird behaviour)
                    if func != self.cancel:
                        if isinstance(cs, Exception):
                            log.warning(
                                f"Task for RPC func {func} failed with"
                                f"{cs}")
                        else:
                            # mark that we have ongoing rpc tasks
                            self._ongoing_rpc_tasks = trio.Event()
                            log.info(f"RPC func is {func}")
                            # store cancel scope such that the rpc task can be
                            # cancelled gracefully if requested
                            self._rpc_tasks[(chan, cid)] = (
                                cs, func, trio.Event())
                    else:
                        # self.cancel() was called so kill this msg loop
                        # and break out into ``_async_main()``
                        log.warning(
                            f"{self.uid} was remotely cancelled; "
                            "waiting on cancellation completion..")
                        await self._cancel_complete.wait()
                        loop_cs.cancel()
                        break

                    log.debug(
                        f"Waiting on next msg for {chan} from {chan.uid}")
                else:
                    # channel disconnect
                    log.debug(
                        f"{chan} for {chan.uid} disconnected, cancelling tasks"
                    )
                    await self.cancel_rpc_tasks(chan)

        except trio.ClosedResourceError:
            log.error(f"{chan} form {chan.uid} broke")
        except (Exception, trio.MultiError) as err:
            # ship any "internal" exception (i.e. one from internal machinery
            # not from an rpc task) to parent
            log.exception("Actor errored:")
            if self._parent_chan:
                await self._parent_chan.send(pack_error(err))
            raise
            # if this is the `MainProcess` we expect the error broadcasting
            # above to trigger an error at consuming portal "checkpoints"
        except trio.Cancelled:
            # debugging only
            log.debug(f"Msg loop was cancelled for {chan}")
            raise
        finally:
            log.debug(
                f"Exiting msg loop for {chan} from {chan.uid} "
                f"with last msg:\n{msg}")

    async def _from_parent(
        self,
        parent_addr: Optional[Tuple[str, int]],
    ) -> Tuple[Channel, Optional[Tuple[str, int]]]:
        try:
            # Connect back to the parent actor and conduct initial
            # handshake. From this point on if we error, we
            # attempt to ship the exception back to the parent.
            chan = Channel(
                destaddr=parent_addr,
            )
            await chan.connect()

            # Initial handshake: swap names.
            await self._do_handshake(chan)

            accept_addr: Optional[Tuple[str, int]] = None

            if self._spawn_method == "trio":
                # Receive runtime state from our parent
                parent_data = await chan.recv()
                log.debug(
                    "Received state from parent:\n"
                    f"{parent_data}"
                )
                accept_addr = (
                    parent_data.pop('bind_host'),
                    parent_data.pop('bind_port'),
                )
                rvs = parent_data.pop('_runtime_vars')
                log.debug(f"Runtime vars are: {rvs}")
                rvs['_is_root'] = False
                _state._runtime_vars.update(rvs)

                for attr, value in parent_data.items():
                    setattr(self, attr, value)

            return chan, accept_addr

        except OSError:  # failed to connect
            log.warning(
                f"Failed to connect to parent @ {parent_addr},"
                " closing server")
            await self.cancel()
            raise

    async def _async_main(
        self,
        accept_addr: Optional[Tuple[str, int]] = None,
        # XXX: currently ``parent_addr`` is only needed for the
        # ``multiprocessing`` backend (which pickles state sent to
        # the child instead of relaying it over the connect-back
        # channel). Once that backend is removed we can likely just
        # change this so a simple ``is_subactor: bool`` which will
        # be False when running as root actor and True when as
        # a subactor.
        parent_addr: Optional[Tuple[str, int]] = None,
        task_status: TaskStatus[None] = trio.TASK_STATUS_IGNORED,
    ) -> None:
        """Start the channel server, maybe connect back to the parent, and
        start the main task.

        A "root-most" (or "top-level") nursery for this actor is opened here
        and when cancelled effectively cancels the actor.
        """
        registered_with_arbiter = False
        try:

            # establish primary connection with immediate parent
            self._parent_chan = None
            if parent_addr is not None:
                self._parent_chan, accept_addr_rent = await self._from_parent(
                    parent_addr)

                # either it's passed in because we're not a child
                # or because we're running in mp mode
                if accept_addr_rent is not None:
                    accept_addr = accept_addr_rent

            # load exposed/allowed RPC modules
            # XXX: do this **after** establishing a channel to the parent
            # but **before** starting the message loop for that channel
            # such that import errors are properly propagated upwards
            self.load_modules()

            # The "root" nursery ensures the channel with the immediate
            # parent is kept alive as a resilient service until
            # cancellation steps have (mostly) occurred in
            # a deterministic way.
            async with trio.open_nursery() as root_nursery:
                self._root_n = root_nursery
                assert self._root_n

                async with trio.open_nursery() as service_nursery:
                    # This nursery is used to handle all inbound
                    # connections to us such that if the TCP server
                    # is killed, connections can continue to process
                    # in the background until this nursery is cancelled.
                    self._service_n = service_nursery
                    assert self._service_n

                    # Startup up the channel server with,
                    # - subactor: the bind address is sent by our parent
                    #   over our established channel
                    # - root actor: the ``accept_addr`` passed to this method
                    assert accept_addr
                    host, port = accept_addr

                    self._server_n = await service_nursery.start(
                        partial(
                            self._serve_forever,
                            service_nursery,
                            accept_host=host,
                            accept_port=port
                        )
                    )
                    accept_addr = self.accept_addr
                    if _state._runtime_vars['_is_root']:
                        _state._runtime_vars['_root_mailbox'] = accept_addr

                    # Register with the arbiter if we're told its addr
                    log.debug(f"Registering {self} for role `{self.name}`")
                    assert isinstance(self._arb_addr, tuple)

                    async with get_arbiter(*self._arb_addr) as arb_portal:
                        await arb_portal.run_from_ns(
                            'self',
                            'register_actor',
                            uid=self.uid,
                            sockaddr=accept_addr,
                        )

                    registered_with_arbiter = True

                    # init steps complete
                    task_status.started()

                    # Begin handling our new connection back to our
                    # parent. This is done last since we don't want to
                    # start processing parent requests until our channel
                    # server is 100% up and running.
                    if self._parent_chan:
                        await root_nursery.start(
                            partial(
                                self._process_messages,
                                self._parent_chan,
                                shield=True,
                            )
                        )
                    log.info("Waiting on service nursery to complete")
                log.info("Service nursery complete")
                log.info("Waiting on root nursery to complete")

            # Blocks here as expected until the root nursery is
            # killed (i.e. this actor is cancelled or signalled by the parent)
        except Exception as err:
            if not registered_with_arbiter:
                # TODO: I guess we could try to connect back
                # to the parent through a channel and engage a debugger
                # once we have that all working with std streams locking?
                log.exception(
                    f"Actor errored and failed to register with arbiter "
                    f"@ {self._arb_addr}?")
                log.error(
                    "\n\n\t^^^ THIS IS PROBABLY A TRACTOR BUGGGGG!!! ^^^\n"
                    "\tCALMLY CALL THE AUTHORITIES AND HIDE YOUR CHILDREN.\n\n"
                    "\tYOUR PARENT CODE IS GOING TO KEEP WORKING FINE!!!\n"
                    "\tTHIS IS HOW RELIABlE SYSTEMS ARE SUPPOSED TO WORK!?!?\n"
                )

            if self._parent_chan:
                with trio.CancelScope(shield=True):
                    try:
                        # internal error so ship to parent without cid
                        await self._parent_chan.send(pack_error(err))
                    except trio.ClosedResourceError:
                        log.error(
                            f"Failed to ship error to parent "
                            f"{self._parent_chan.uid}, channel was closed")

            # always!
            log.exception("Actor errored:")
            raise

        finally:
            log.info("Root nursery complete")

            # tear down all lifetime contexts if not in guest mode
            # XXX: should this just be in the entrypoint?
            log.warning("Closing all actor lifetime contexts")
            _lifetime_stack.close()

            # Unregister actor from the arbiter
            if registered_with_arbiter and (
                    self._arb_addr is not None
            ):
                failed = False
                with trio.move_on_after(0.5) as cs:
                    cs.shield = True
                    try:
                        async with get_arbiter(*self._arb_addr) as arb_portal:
                            await arb_portal.run_from_ns(
                                'self',
                                'unregister_actor',
                                uid=self.uid
                            )
                    except OSError:
                        failed = True
                if cs.cancelled_caught:
                    failed = True
                if failed:
                    log.warning(
                        f"Failed to unregister {self.name} from arbiter")

            # Ensure all peers (actors connected to us as clients) are finished
            if not self._no_more_peers.is_set():
                if any(
                    chan.connected() for chan in chain(*self._peers.values())
                ):
                    log.debug(
                        f"Waiting for remaining peers {self._peers} to clear")
                    with trio.CancelScope(shield=True):
                        await self._no_more_peers.wait()
            log.debug("All peer channels are complete")

        log.debug("Runtime completed")

    async def _serve_forever(
        self,
        handler_nursery: trio.Nursery,
        *,
        # (host, port) to bind for channel server
        accept_host: Tuple[str, int] = None,
        accept_port: int = 0,
        task_status: TaskStatus[trio.Nursery] = trio.TASK_STATUS_IGNORED,
    ) -> None:
        """Start the channel server, begin listening for new connections.

        This will cause an actor to continue living (blocking) until
        ``cancel_server()`` is called.
        """
        self._server_down = trio.Event()
        try:
            async with trio.open_nursery() as server_n:
                l: List[trio.abc.Listener] = await server_n.start(
                    partial(
                        trio.serve_tcp,
                        self._stream_handler,
                        # new connections will stay alive even if this server
                        # is cancelled
                        handler_nursery=handler_nursery,
                        port=accept_port,
                        host=accept_host,
                    )
                )
                log.debug(
                    "Started tcp server(s) on"
                    f" {[getattr(l, 'socket', 'unknown socket') for l in l]}")
                self._listeners.extend(l)
                task_status.started(server_n)
        finally:
            # signal the server is down since nursery above terminated
            self._server_down.set()

    def cancel_soon(self) -> None:
        """Cancel this actor asap; can be called from a sync context.

        Schedules `.cancel()` to be run immediately just like when
        cancelled by the parent.
        """
        self._service_n.start_soon(self.cancel)

    async def cancel(self) -> bool:
        """Cancel this actor.

        The "deterministic" teardown sequence in order is:
            - cancel all ongoing rpc tasks by cancel scope
            - cancel the channel server to prevent new inbound
              connections
            - cancel the "service" nursery reponsible for
              spawning new rpc tasks
            - return control the parent channel message loop
        """
        log.warning(f"{self.uid} is trying to cancel")
        self._cancel_called = True

        # cancel all ongoing rpc tasks
        with trio.CancelScope(shield=True):

            # kill any debugger request task to avoid deadlock
            # with the root actor in this tree
            dbcs = _debug._debugger_request_cs
            if dbcs is not None:
                log.debug("Cancelling active debugger request")
                dbcs.cancel()

            # kill all ongoing tasks
            await self.cancel_rpc_tasks()

            # stop channel server
            self.cancel_server()
            await self._server_down.wait()

            # cancel all rpc tasks permanently
            if self._service_n:
                self._service_n.cancel_scope.cancel()

        log.warning(f"{self.uid} was sucessfullly cancelled")
        self._cancel_complete.set()
        return True

    # XXX: hard kill logic if needed?
    # def _hard_mofo_kill(self):
    #     # If we're the root actor or zombied kill everything
    #     if self._parent_chan is None:  # TODO: more robust check
    #         root = trio.lowlevel.current_root_task()
    #         for n in root.child_nurseries:
    #             n.cancel_scope.cancel()

    async def _cancel_task(self, cid, chan):
        """Cancel a local task by call-id / channel.

        Note this method will be treated as a streaming function
        by remote actor-callers due to the declaration of ``ctx``
        in the signature (for now).
        """
        # right now this is only implicitly called by
        # streaming IPC but it should be called
        # to cancel any remotely spawned task
        try:
            # this ctx based lookup ensures the requested task to
            # be cancelled was indeed spawned by a request from this channel
            scope, func, is_complete = self._rpc_tasks[(chan, cid)]
        except KeyError:
            log.warning(f"{cid} has already completed/terminated?")
            return

        log.debug(
            f"Cancelling task:\ncid: {cid}\nfunc: {func}\n"
            f"peer: {chan.uid}\n")

        # don't allow cancelling this function mid-execution
        # (is this necessary?)
        if func is self._cancel_task:
            return

        scope.cancel()
        # wait for _invoke to mark the task complete
        log.debug(
            f"Waiting on task to cancel:\ncid: {cid}\nfunc: {func}\n"
            f"peer: {chan.uid}\n")
        await is_complete.wait()
        log.debug(
            f"Sucessfully cancelled task:\ncid: {cid}\nfunc: {func}\n"
            f"peer: {chan.uid}\n")

    async def cancel_rpc_tasks(
        self,
        only_chan: Optional[Channel] = None,
    ) -> None:
        """Cancel all existing RPC responder tasks using the cancel scope
        registered for each.
        """
        tasks = self._rpc_tasks
        log.info(f"Cancelling all {len(tasks)} rpc tasks:\n{tasks} ")
        for (chan, cid) in tasks.copy():
            if only_chan is not None:
                if only_chan != chan:
                    continue

            # TODO: this should really done in a nursery batch
            await self._cancel_task(cid, chan)

        log.info(
            f"Waiting for remaining rpc tasks to complete {tasks}")
        await self._ongoing_rpc_tasks.wait()

    def cancel_server(self) -> None:
        """Cancel the internal channel server nursery thereby
        preventing any new inbound connections from being established.
        """
        if self._server_n:
            log.debug("Shutting down channel server")
            self._server_n.cancel_scope.cancel()

    @property
    def accept_addr(self) -> Optional[Tuple[str, int]]:
        """Primary address to which the channel server is bound.
        """
        # throws OSError on failure
        return self._listeners[0].socket.getsockname()  # type: ignore

    def get_parent(self) -> Portal:
        """Return a portal to our parent actor."""
        assert self._parent_chan, "No parent channel for this actor?"
        return Portal(self._parent_chan)

    def get_chans(self, uid: Tuple[str, str]) -> List[Channel]:
        """Return all channels to the actor with provided uid."""
        return self._peers[uid]

    async def _do_handshake(
        self,
        chan: Channel
    ) -> Tuple[str, str]:
        """Exchange (name, UUIDs) identifiers as the first communication step.

        These are essentially the "mailbox addresses" found in actor model
        parlance.
        """
        await chan.send(self.uid)
        uid: Tuple[str, str] = await chan.recv()

        if not isinstance(uid, tuple):
            raise ValueError(f"{uid} is not a valid uid?!")

        chan.uid = uid
        log.info(f"Handshake with actor {uid}@{chan.raddr} complete")
        return uid


class Arbiter(Actor):
    """A special actor who knows all the other actors and always has
    access to a top level nursery.

    The arbiter is by default the first actor spawned on each host
    and is responsible for keeping track of all other actors for
    coordination purposes. If a new main process is launched and an
    arbiter is already running that arbiter will be used.
    """
    is_arbiter = True

    def __init__(self, *args, **kwargs):
        self._registry = defaultdict(list)
        self._waiters = {}
        super().__init__(*args, **kwargs)

    def find_actor(self, name: str) -> Optional[Tuple[str, int]]:
        for uid, sockaddr in self._registry.items():
            if name in uid:
                return sockaddr

        return None

    async def get_registry(
        self
    ) -> Dict[str, Tuple[str, str]]:
        """Return current name registry.
        """
        # NOTE: requires ``strict_map_key=False`` to the msgpack
        # unpacker since we have tuples as keys (not this makes the
        # arbiter suscetible to hashdos):
        # https://github.com/msgpack/msgpack-python#major-breaking-changes-in-msgpack-10
        return self._registry

    async def wait_for_actor(
        self, name: str
    ) -> List[Tuple[str, int]]:
        """Wait for a particular actor to register.

        This is a blocking call if no actor by the provided name is currently
        registered.
        """
        sockaddrs = []

        for (aname, _), sockaddr in self._registry.items():
            if name == aname:
                sockaddrs.append(sockaddr)

        if not sockaddrs:
            waiter = trio.Event()
            self._waiters.setdefault(name, []).append(waiter)
            await waiter.wait()
            for uid in self._waiters[name]:
                sockaddrs.append(self._registry[uid])

        return sockaddrs

    def register_actor(
        self, uid: Tuple[str, str], sockaddr: Tuple[str, int]
    ) -> None:
        name, uuid = uid
        self._registry[uid] = sockaddr

        # pop and signal all waiter events
        events = self._waiters.pop(name, ())
        self._waiters.setdefault(name, []).append(uid)
        for event in events:
            if isinstance(event, trio.Event):
                event.set()

    def unregister_actor(self, uid: Tuple[str, str]) -> None:
        self._registry.pop(uid, None)

"""
``trio`` inspired apis and helpers
"""
from functools import partial
import inspect
import multiprocessing as mp
from typing import Tuple, List, Dict, Optional
import typing
import warnings

import trio
from async_generator import asynccontextmanager

from ._state import current_actor, is_main_process
from .log import get_logger, get_loglevel
from ._actor import Actor
from ._portal import Portal
from ._exceptions import is_multi_cancelled
from ._root import open_root_actor
from . import _state
from . import _spawn


log = get_logger(__name__)

_default_bind_addr: Tuple[str, int] = ('127.0.0.1', 0)


class ActorNursery:
    """Spawn scoped subprocess actors.
    """
    def __init__(
        self,
        actor: Actor,
        ria_nursery: trio.Nursery,
        da_nursery: trio.Nursery,
        errors: Dict[Tuple[str, str], Exception],
    ) -> None:
        # self.supervisor = supervisor  # TODO
        self._actor: Actor = actor
        self._ria_nursery = ria_nursery
        self._da_nursery = da_nursery
        self._children: Dict[
            Tuple[str, str],
            Tuple[Actor, mp.Process, Optional[Portal]]
        ] = {}
        # portals spawned with ``run_in_actor()`` are
        # cancelled when their "main" result arrives
        self._cancel_after_result_on_exit: set = set()
        self.cancelled: bool = False
        self._join_procs = trio.Event()
        self.errors = errors

    async def start_actor(
        self,
        name: str,
        *,
        bind_addr: Tuple[str, int] = _default_bind_addr,
        rpc_module_paths: List[str] = None,
        enable_modules: List[str] = None,
        loglevel: str = None,  # set log level per subactor
        nursery: trio.Nursery = None,
    ) -> Portal:
        loglevel = loglevel or self._actor.loglevel or get_loglevel()

        # configure and pass runtime state
        _rtv = _state._runtime_vars.copy()
        _rtv['_is_root'] = False

        enable_modules = enable_modules or []

        if rpc_module_paths:
            warnings.warn(
                "`rpc_module_paths` is now deprecated, use "
                " `enable_modules` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            enable_modules.extend(rpc_module_paths)

        subactor = Actor(
            name,
            # modules allowed to invoked funcs from
            enable_modules=enable_modules,
            loglevel=loglevel,
            arbiter_addr=current_actor()._arb_addr,
        )
        parent_addr = self._actor.accept_addr
        assert parent_addr

        # start a task to spawn a process
        # blocks until process has been started and a portal setup
        nursery = nursery or self._da_nursery

        # XXX: the type ignore is actually due to a `mypy` bug
        return await nursery.start(  # type: ignore
            partial(
                _spawn.new_proc,
                name,
                self,
                subactor,
                self.errors,
                bind_addr,
                parent_addr,
                _rtv,  # run time vars
            )
        )

    async def run_in_actor(
        self,
        fn: typing.Callable,
        *,
        name: Optional[str] = None,
        bind_addr: Tuple[str, int] = _default_bind_addr,
        rpc_module_paths: Optional[List[str]] = None,
        enable_modules: List[str] = None,
        loglevel: str = None,  # set log level per subactor
        **kwargs,  # explicit args to ``fn``
    ) -> Portal:
        """Spawn a new actor, run a lone task, then terminate the actor and
        return its result.

        Actors spawned using this method are kept alive at nursery teardown
        until the task spawned by executing ``fn`` completes at which point
        the actor is terminated.
        """
        mod_path = fn.__module__

        if name is None:
            # use the explicit function name if not provided
            name = fn.__name__

        portal = await self.start_actor(
            name,
            enable_modules=[mod_path] + (enable_modules or rpc_module_paths or []),
            bind_addr=bind_addr,
            loglevel=loglevel,
            # use the run_in_actor nursery
            nursery=self._ria_nursery,
        )

        # XXX: don't allow stream funcs
        if not (
            inspect.iscoroutinefunction(fn) and
            not getattr(fn, '_tractor_stream_function', False)
        ):
            raise TypeError(f'{fn} must be an async function!')

        # this marks the actor to be cancelled after its portal result
        # is retreived, see logic in `open_nursery()` below.
        self._cancel_after_result_on_exit.add(portal)
        await portal._submit_for_result(
            mod_path,
            fn.__name__,
            **kwargs
        )
        return portal

    async def cancel(self, hard_kill: bool = False) -> None:
        """Cancel this nursery by instructing each subactor to cancel
        itself and wait for all subactors to terminate.

        If ``hard_killl`` is set to ``True`` then kill the processes
        directly without any far end graceful ``trio`` cancellation.
        """
        self.cancelled = True

        log.warning(f"Cancelling nursery in {self._actor.uid}")
        with trio.move_on_after(3) as cs:
            async with trio.open_nursery() as nursery:
                for subactor, proc, portal in self._children.values():
                    if hard_kill:
                        proc.terminate()
                    else:
                        if portal is None:  # actor hasn't fully spawned yet
                            event = self._actor._peer_connected[subactor.uid]
                            log.warning(
                                f"{subactor.uid} wasn't finished spawning?")
                            await event.wait()
                            # channel/portal should now be up
                            _, _, portal = self._children[subactor.uid]

                            # XXX should be impossible to get here
                            # unless method was called from within
                            # shielded cancel scope.
                            if portal is None:
                                # cancelled while waiting on the event
                                # to arrive
                                chan = self._actor._peers[subactor.uid][-1]
                                if chan:
                                    portal = Portal(chan)
                                else:  # there's no other choice left
                                    proc.terminate()

                        # spawn cancel tasks for each sub-actor
                        assert portal
                        nursery.start_soon(portal.cancel_actor)

        # if we cancelled the cancel (we hung cancelling remote actors)
        # then hard kill all sub-processes
        if cs.cancelled_caught:
            log.error(
                f"Failed to cancel {self}\nHard killing process tree!")
            for subactor, proc, portal in self._children.values():
                log.warning(f"Hard killing process {proc}")
                proc.terminate()

        # mark ourselves as having (tried to have) cancelled all subactors
        self._join_procs.set()


@asynccontextmanager
async def _open_and_supervise_one_cancels_all_nursery(
    actor: Actor,
) -> typing.AsyncGenerator[ActorNursery, None]:

    # the collection of errors retreived from spawned sub-actors
    errors: Dict[Tuple[str, str], Exception] = {}

    # This is the outermost level "deamon actor" nursery. It is awaited
    # **after** the below inner "run in actor nursery". This allows for
    # handling errors that are generated by the inner nursery in
    # a supervisor strategy **before** blocking indefinitely to wait for
    # actors spawned in "daemon mode" (aka started using
    # ``ActorNursery.start_actor()``).

    # errors from this daemon actor nursery bubble up to caller
    async with trio.open_nursery() as da_nursery:
        try:
            # This is the inner level "run in actor" nursery. It is
            # awaited first since actors spawned in this way (using
            # ``ActorNusery.run_in_actor()``) are expected to only
            # return a single result and then complete (i.e. be canclled
            # gracefully). Errors collected from these actors are
            # immediately raised for handling by a supervisor strategy.
            # As such if the strategy propagates any error(s) upwards
            # the above "daemon actor" nursery will be notified.
            async with trio.open_nursery() as ria_nursery:
                anursery = ActorNursery(
                    actor,
                    ria_nursery,
                    da_nursery,
                    errors
                )
                try:
                    # spawning of actors happens in the caller's scope
                    # after we yield upwards
                    yield anursery
                    log.debug(
                        f"Waiting on subactors {anursery._children} "
                        "to complete"
                    )
                except BaseException as err:
                    # if the caller's scope errored then we activate our
                    # one-cancels-all supervisor strategy (don't
                    # worry more are coming).
                    anursery._join_procs.set()
                    try:
                        # XXX: hypothetically an error could be
                        # raised and then a cancel signal shows up
                        # slightly after in which case the `else:`
                        # block here might not complete?  For now,
                        # shield both.
                        with trio.CancelScope(shield=True):
                            etype = type(err)
                            if etype in (
                                trio.Cancelled,
                                KeyboardInterrupt
                            ) or (
                                is_multi_cancelled(err)
                            ):
                                log.warning(
                                    f"Nursery for {current_actor().uid} "
                                    f"was cancelled with {etype}")
                            else:
                                log.exception(
                                    f"Nursery for {current_actor().uid} "
                                    f"errored with {err}, ")

                            # cancel all subactors
                            await anursery.cancel()

                    except trio.MultiError as merr:
                        # If we receive additional errors while waiting on
                        # remaining subactors that were cancelled,
                        # aggregate those errors with the original error
                        # that triggered this teardown.
                        if err not in merr.exceptions:
                            raise trio.MultiError(merr.exceptions + [err])
                    else:
                        raise

                # Last bit before first nursery block ends in the case
                # where we didn't error in the caller's scope
                log.debug("Waiting on all subactors to complete")
                anursery._join_procs.set()

                # ria_nursery scope end

        # XXX: do we need a `trio.Cancelled` catch here as well?
        except (Exception, trio.MultiError, trio.Cancelled) as err:
            # If actor-local error was raised while waiting on
            # ".run_in_actor()" actors then we also want to cancel all
            # remaining sub-actors (due to our lone strategy:
            # one-cancels-all).
            log.warning(f"Nursery cancelling due to {err}")
            if anursery._children:
                with trio.CancelScope(shield=True):
                    await anursery.cancel()
            raise
        finally:
            # No errors were raised while awaiting ".run_in_actor()"
            # actors but those actors may have returned remote errors as
            # results (meaning they errored remotely and have relayed
            # those errors back to this parent actor). The errors are
            # collected in ``errors`` so cancel all actors, summarize
            # all errors and re-raise.
            if errors:
                if anursery._children:
                    with trio.CancelScope(shield=True):
                        await anursery.cancel()

                # use `MultiError` as needed
                if len(errors) > 1:
                    raise trio.MultiError(tuple(errors.values()))
                else:
                    raise list(errors.values())[0]

        # ria_nursery scope end - nursery checkpoint

    # after nursery exit


@asynccontextmanager
async def open_nursery(
    **kwargs,
) -> typing.AsyncGenerator[ActorNursery, None]:
    """Create and yield a new ``ActorNursery`` to be used for spawning
    structured concurrent subactors.

    When an actor is spawned a new trio task is started which
    invokes one of the process spawning backends to create and start
    a new subprocess. These tasks are started by one of two nurseries
    detailed below. The reason for spawning processes from within
    a new task is because ``trio_run_in_process`` itself creates a new
    internal nursery and the same task that opens a nursery **must**
    close it. It turns out this approach is probably more correct
    anyway since it is more clear from the following nested nurseries
    which cancellation scopes correspond to each spawned subactor set.
    """
    implicit_runtime = False

    actor = current_actor(err_on_no_runtime=False)

    try:
        if actor is None and is_main_process():

            # if we are the parent process start the actor runtime implicitly
            log.info("Starting actor runtime!")

            # mark us for teardown on exit
            implicit_runtime = True

            async with open_root_actor(**kwargs) as actor:
                assert actor is current_actor()

                async with _open_and_supervise_one_cancels_all_nursery(
                    actor
                ) as anursery:

                    yield anursery

        else:  # sub-nursery case

            async with _open_and_supervise_one_cancels_all_nursery(
                actor
            ) as anursery:

                yield anursery

    finally:
        log.debug("Nursery teardown complete")

        # shutdown runtime if it was started
        if implicit_runtime:
            log.info("Shutting down actor tree")

"""
Machinery for actor process spawning using multiple backends.
"""
import sys
import multiprocessing as mp
import platform
from typing import Any, Dict, Optional

import trio
from trio_typing import TaskStatus
from async_generator import asynccontextmanager

try:
    from multiprocessing import semaphore_tracker  # type: ignore
    resource_tracker = semaphore_tracker
    resource_tracker._resource_tracker = resource_tracker._semaphore_tracker
except ImportError:
    # 3.8 introduces a more general version that also tracks shared mems
    from multiprocessing import resource_tracker  # type: ignore

from multiprocessing import forkserver  # type: ignore
from typing import Tuple

from . import _forkserver_override
from ._state import current_actor, is_main_process
from .log import get_logger
from ._portal import Portal
from ._actor import Actor, ActorFailure
from ._entry import _mp_main


log = get_logger('tractor')

# placeholder for an mp start context if so using that backend
_ctx: Optional[mp.context.BaseContext] = None
_spawn_method: str = "spawn"


if platform.system() == 'Windows':
    _spawn_method = "spawn"
    _ctx = mp.get_context("spawn")

    async def proc_waiter(proc: mp.Process) -> None:
        await trio.lowlevel.WaitForSingleObject(proc.sentinel)
else:
    # *NIX systems use ``trio`` primitives as our default
    _spawn_method = "trio"

    async def proc_waiter(proc: mp.Process) -> None:
        await trio.lowlevel.wait_readable(proc.sentinel)


def try_set_start_method(name: str) -> Optional[mp.context.BaseContext]:
    """Attempt to set the method for process starting, aka the "actor
    spawning backend".

    If the desired method is not supported this function will error.
    On Windows only the ``multiprocessing`` "spawn" method is offered
    besides the default ``trio`` which uses async wrapping around
    ``subprocess.Popen``.
    """
    global _ctx
    global _spawn_method

    methods = mp.get_all_start_methods()
    if 'fork' in methods:
        # forking is incompatible with ``trio``s global task tree
        methods.remove('fork')

    # supported on all platforms
    methods += ['trio']

    if name not in methods:
        raise ValueError(
            f"Spawn method `{name}` is invalid please choose one of {methods}"
        )
    elif name == 'forkserver':
        _forkserver_override.override_stdlib()
        _ctx = mp.get_context(name)
    elif name == 'trio':
        _ctx = None
    else:
        _ctx = mp.get_context(name)

    _spawn_method = name
    return _ctx


async def exhaust_portal(
    portal: Portal,
    actor: Actor
) -> Any:
    """Pull final result from portal (assuming it has one).

    If the main task is an async generator do our best to consume
    what's left of it.
    """
    try:
        log.debug(f"Waiting on final result from {actor.uid}")

        # XXX: streams should never be reaped here since they should
        # always be established and shutdown using a context manager api
        final = await portal.result()

    except (Exception, trio.MultiError) as err:
        # we reraise in the parent task via a ``trio.MultiError``
        return err
    except trio.Cancelled as err:
        # lol, of course we need this too ;P
        # TODO: merge with above?
        log.warning(f"Cancelled result waiter for {portal.actor.uid}")
        return err
    else:
        log.debug(f"Returning final result: {final}")
        return final


async def cancel_on_completion(
    portal: Portal,
    actor: Actor,
    errors: Dict[Tuple[str, str], Exception],
    task_status: TaskStatus[trio.CancelScope] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Cancel actor gracefully once it's "main" portal's
    result arrives.

    Should only be called for actors spawned with `run_in_actor()`.
    """
    with trio.CancelScope() as cs:

        task_status.started(cs)

        # if this call errors we store the exception for later
        # in ``errors`` which will be reraised inside
        # a MultiError and we still send out a cancel request
        result = await exhaust_portal(portal, actor)
        if isinstance(result, Exception):
            errors[actor.uid] = result
            log.warning(
                f"Cancelling {portal.channel.uid} after error {result}"
            )

        else:
            log.info(
                f"Cancelling {portal.channel.uid} gracefully "
                f"after result {result}")

        # cancel the process now that we have a final result
        await portal.cancel_actor()


@asynccontextmanager
async def spawn_subactor(
    subactor: 'Actor',
    parent_addr: Tuple[str, int],
    infect_asyncio: bool,
):
    spawn_cmd = [
        sys.executable,
        "-m",
        # Hardcode this (instead of using ``_child.__name__`` to avoid a
        # double import warning: https://stackoverflow.com/a/45070583
        "tractor._child",
        # We provide the child's unique identifier on this exec/spawn
        # line for debugging purposes when viewing the process tree from
        # the OS; it otherwise can be passed via the parent channel if
        # we prefer in the future (for privacy).
        "--uid",
        str(subactor.uid),
        # Address the child must connect to on startup
        "--parent_addr",
        str(parent_addr)
    ]

    if subactor.loglevel:
        spawn_cmd += [
            "--loglevel",
            subactor.loglevel
        ]

    # Tell child to run in guest mode on top of ``asyncio`` loop
    if infect_asyncio:
        spawn_cmd.append("--asyncio")

    proc = await trio.open_process(spawn_cmd)
    try:
        yield proc
    finally:
        # XXX: do this **after** cancellation/tearfown
        # to avoid killing the process too early
        # since trio does this internally on ``__aexit__()``

        # NOTE: we always "shield" join sub procs in
        # the outer scope since no actor zombies are
        # ever allowed. This ``__aexit__()`` also shields
        # internally.
        log.debug(f"Attempting to kill {proc}")

        # NOTE: this timeout effectively does nothing right now since
        # we are shielding the ``.wait()`` inside ``new_proc()`` which
        # will pretty much never release until the process exits.
        with trio.move_on_after(3) as cs:
            async with proc:
                log.debug(f"Terminating {proc}")
        if cs.cancelled_caught:
            log.critical(f"HARD KILLING {proc}")
            proc.kill()


async def new_proc(
    name: str,
    actor_nursery: 'ActorNursery',  # type: ignore  # noqa
    subactor: Actor,
    errors: Dict[Tuple[str, str], Exception],
    # passed through to actor main
    bind_addr: Tuple[str, int],
    parent_addr: Tuple[str, int],
    _runtime_vars: Dict[str, Any],  # serialized and sent to _child
    *,
    use_trio_run_in_process: bool = False,
    infect_asyncio: bool = False,
    task_status: TaskStatus[Portal] = trio.TASK_STATUS_IGNORED
) -> None:
    """Create a new ``multiprocessing.Process`` using the
    spawn method as configured using ``try_set_start_method()``.
    """
    cancel_scope = None

    # mark the new actor with the global spawn method
    subactor._spawn_method = _spawn_method

    if use_trio_run_in_process or _spawn_method == 'trio':
        async with trio.open_nursery() as nursery:
            async with spawn_subactor(
                subactor,
                parent_addr,
                infect_asyncio=infect_asyncio
            ) as proc:
                log.info(f"Started {proc}")

                # wait for actor to spawn and connect back to us
                # channel should have handshake completed by the
                # local actor by the time we get a ref to it
                event, chan = await actor_nursery._actor.wait_for_peer(
                    subactor.uid)
                portal = Portal(chan)
                actor_nursery._children[subactor.uid] = (
                    subactor, proc, portal)

                # send additional init params
                await chan.send({
                    "_parent_main_data": subactor._parent_main_data,
                    "enable_modules": subactor.enable_modules,
                    "_arb_addr": subactor._arb_addr,
                    "bind_host": bind_addr[0],
                    "bind_port": bind_addr[1],
                    "_runtime_vars": _runtime_vars,
                })

                # track subactor in current nursery
                curr_actor = current_actor()
                curr_actor._actoruid2nursery[subactor.uid] = actor_nursery

                # resume caller at next checkpoint now that child is up
                task_status.started(portal)

                # wait for ActorNursery.wait() to be called
                with trio.CancelScope(shield=True):
                    await actor_nursery._join_procs.wait()

                if portal in actor_nursery._cancel_after_result_on_exit:
                    cancel_scope = await nursery.start(
                        cancel_on_completion,
                        portal,
                        subactor,
                        errors
                    )

                # Wait for proc termination but **dont' yet** call
                # ``trio.Process.__aexit__()`` (it tears down stdio
                # which will kill any waiting remote pdb trace).

                # TODO: No idea how we can enforce zombie
                # reaping more stringently without the shield
                # we used to have below...

                # always "hard" join sub procs:
                # no actor zombies allowed
                # with trio.CancelScope(shield=True):
                await proc.wait()

            log.debug(f"Joined {proc}")
            # pop child entry to indicate we no longer managing this subactor
            subactor, proc, portal = actor_nursery._children.pop(subactor.uid)

            # cancel result waiter that may have been spawned in
            # tandem if not done already
            if cancel_scope:
                log.warning(
                    "Cancelling existing result waiter task for "
                    f"{subactor.uid}")
                cancel_scope.cancel()
    else:
        # `multiprocessing`
        # async with trio.open_nursery() as nursery:
        await mp_new_proc(
            name=name,
            actor_nursery=actor_nursery,
            subactor=subactor,
            errors=errors,
            # passed through to actor main
            bind_addr=bind_addr,
            parent_addr=parent_addr,
            _runtime_vars=_runtime_vars,
            task_status=task_status,
        )


async def mp_new_proc(

    name: str,
    actor_nursery: 'ActorNursery',  # type: ignore  # noqa
    subactor: Actor,
    errors: Dict[Tuple[str, str], Exception],
    # passed through to actor main
    bind_addr: Tuple[str, int],
    parent_addr: Tuple[str, int],
    _runtime_vars: Dict[str, Any],  # serialized and sent to _child
    *,
    use_trio_run_in_process: bool = False,
    task_status: TaskStatus[Portal] = trio.TASK_STATUS_IGNORED

) -> None:
    async with trio.open_nursery() as nursery:
        assert _ctx
        start_method = _ctx.get_start_method()
        if start_method == 'forkserver':
            # XXX do our hackery on the stdlib to avoid multiple
            # forkservers (one at each subproc layer).
            fs = forkserver._forkserver
            curr_actor = current_actor()
            if is_main_process() and not curr_actor._forkserver_info:
                # if we're the "main" process start the forkserver
                # only once and pass its ipc info to downstream
                # children
                # forkserver.set_forkserver_preload(enable_modules)
                forkserver.ensure_running()
                fs_info = (
                    fs._forkserver_address,
                    fs._forkserver_alive_fd,
                    getattr(fs, '_forkserver_pid', None),
                    getattr(
                        resource_tracker._resource_tracker, '_pid', None),
                    resource_tracker._resource_tracker._fd,
                )
            else:
                assert curr_actor._forkserver_info
                fs_info = (
                    fs._forkserver_address,
                    fs._forkserver_alive_fd,
                    fs._forkserver_pid,
                    resource_tracker._resource_tracker._pid,
                    resource_tracker._resource_tracker._fd,
                 ) = curr_actor._forkserver_info
        else:
            fs_info = (None, None, None, None, None)

        proc: mp.Process = _ctx.Process(  # type: ignore
            target=_mp_main,
            args=(
                subactor,
                bind_addr,
                fs_info,
                start_method,
                parent_addr,
                infect_asyncio,
            ),
            # daemon=True,
            name=name,
        )
        # `multiprocessing` only (since no async interface):
        # register the process before start in case we get a cancel
        # request before the actor has fully spawned - then we can wait
        # for it to fully come up before sending a cancel request
        actor_nursery._children[subactor.uid] = (subactor, proc, None)

        proc.start()
        if not proc.is_alive():
            raise ActorFailure("Couldn't start sub-actor?")

        log.info(f"Started {proc}")

        try:
            # wait for actor to spawn and connect back to us
            # channel should have handshake completed by the
            # local actor by the time we get a ref to it
            event, chan = await actor_nursery._actor.wait_for_peer(
                subactor.uid)
            portal = Portal(chan)
            actor_nursery._children[subactor.uid] = (subactor, proc, portal)

            # unblock parent task
            task_status.started(portal)

            # wait for ``ActorNursery`` block to signal that
            # subprocesses can be waited upon.
            # This is required to ensure synchronization
            # with user code that may want to manually await results
            # from nursery spawned sub-actors. We don't want the
            # containing nurseries here to collect results or error
            # while user code is still doing it's thing. Only after the
            # nursery block closes do we allow subactor results to be
            # awaited and reported upwards to the supervisor.
            await actor_nursery._join_procs.wait()

        finally:
            # XXX: in the case we were cancelled before the sub-proc
            # registered itself back we must be sure to try and clean
            # any process we may have started.

            reaping_cancelled: bool = False
            cancel_scope: Optional[trio.CancelScope] = None
            cancel_exc: Optional[trio.Cancelled] = None

            if portal in actor_nursery._cancel_after_result_on_exit:
                try:
                    # async with trio.open_nursery() as n:
                    # n.cancel_scope.shield = True
                    cancel_scope = await nursery.start(
                        cancel_on_completion,
                        portal,
                        subactor,
                        errors
                    )
                except trio.Cancelled as err:
                    cancel_exc = err

                    # if the reaping task was cancelled we may have hit
                    # a race where the subproc disconnected before we
                    # could send it a message to cancel (classic 2 generals)
                    # in that case, wait shortly then kill the process.
                    reaping_cancelled = True

                    if proc.is_alive():
                        with trio.move_on_after(0.1) as cs:
                            cs.shield = True
                            await proc_waiter(proc)

                        if cs.cancelled_caught:
                            proc.terminate()

            if not reaping_cancelled and proc.is_alive():
                await proc_waiter(proc)

            # TODO: timeout block here?
            proc.join()

            log.debug(f"Joined {proc}")
            # pop child entry to indicate we are no longer managing subactor
            subactor, proc, portal = actor_nursery._children.pop(subactor.uid)

            # cancel result waiter that may have been spawned in
            # tandem if not done already
            if cancel_scope:
                log.warning(
                    "Cancelling existing result waiter task for "
                    f"{subactor.uid}")
                cancel_scope.cancel()

            elif reaping_cancelled:  # let the cancellation bubble up
                assert cancel_exc
                raise cancel_exc

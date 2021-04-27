"""
Root actor runtime ignition(s).
"""
from contextlib import asynccontextmanager
from functools import partial
import importlib
import os
from typing import Tuple, Optional, List, Any
import typing
import warnings

import trio

from ._actor import Actor, Arbiter
from . import _debug
from . import _spawn
from . import _state
from . import log
from ._ipc import _connect_chan


# set at startup and after forks
_default_arbiter_host = '127.0.0.1'
_default_arbiter_port = 1616


logger = log.get_logger('tractor')


@asynccontextmanager
async def open_root_actor(

    # defaults are above
    arbiter_addr: Tuple[str, int] = (
        _default_arbiter_host,
        _default_arbiter_port,
    ),

    name: Optional[str] = 'root',

    # either the `multiprocessing` start method:
    # https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
    # OR `trio` (the new default).
    start_method: Optional[str] = None,

    # enables the multi-process debugger support
    debug_mode: bool = False,

    # internal logging
    loglevel: Optional[str] = None,

    enable_modules: Optional[List] = None,
    rpc_module_paths: Optional[List] = None,

) -> typing.Any:
    """Async entry point for ``tractor``.

    """
    # Override the global debugger hook to make it play nice with
    # ``trio``, see:
    # https://github.com/python-trio/trio/issues/1155#issuecomment-742964018
    os.environ['PYTHONBREAKPOINT'] = 'tractor._debug._set_trace'

    # mark top most level process as root actor
    _state._runtime_vars['_is_root'] = True

    # caps based rpc list
    enable_modules = enable_modules or []

    if rpc_module_paths:
        warnings.warn(
            "`rpc_module_paths` is now deprecated, use "
            " `enable_modules` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        enable_modules.extend(rpc_module_paths)

    if start_method is not None:
        _spawn.try_set_start_method(start_method)

    if debug_mode and _spawn._spawn_method == 'trio':
        _state._runtime_vars['_debug_mode'] = True

        # expose internal debug module to every actor allowing
        # for use of ``await tractor.breakpoint()``
        enable_modules.append('tractor._debug')

    elif debug_mode:
        raise RuntimeError(
            "Debug mode is only supported for the `trio` backend!"
        )

    arbiter_addr = (host, port) = arbiter_addr or (
        _default_arbiter_host,
        _default_arbiter_port
    )

    loglevel = loglevel or log.get_loglevel()
    if loglevel is not None:
        log._default_loglevel = loglevel
        log.get_console_log(loglevel)

    # make a temporary connection to see if an arbiter exists
    arbiter_found = False

    try:
        async with _connect_chan(host, port):
            arbiter_found = True

    except OSError:
        logger.warning(f"No actor could be found @ {host}:{port}")

    # create a local actor and start up its main routine/task
    if arbiter_found:

        # we were able to connect to an arbiter
        logger.info(f"Arbiter seems to exist @ {host}:{port}")

        actor = Actor(
            name or 'anonymous',
            arbiter_addr=arbiter_addr,
            loglevel=loglevel,
            enable_modules=enable_modules,
        )
        host, port = (host, 0)

    else:
        # start this local actor as the arbiter (aka a regular actor who
        # manages the local registry of "mailboxes")

        # Note that if the current actor is the arbiter it is desirable
        # for it to stay up indefinitely until a re-election process has
        # taken place - which is not implemented yet FYI).

        actor = Arbiter(
            name or 'arbiter',
            arbiter_addr=arbiter_addr,
            loglevel=loglevel,
            enable_modules=enable_modules,
        )

    try:
        # assign process-local actor
        _state._current_actor = actor

        # start local channel-server and fake the portal API
        # NOTE: this won't block since we provide the nursery
        logger.info(f"Starting local {actor} @ {host}:{port}")

        # start the actor runtime in a new task
        async with trio.open_nursery() as nursery:

            # ``Actor._async_main()`` creates an internal nursery and
            # thus blocks here until the entire underlying actor tree has
            # terminated thereby conducting structured concurrency.

            await nursery.start(
                partial(
                    actor._async_main,
                    accept_addr=(host, port),
                    parent_addr=None
                )
            )
            try:
                yield actor

            except (Exception, trio.MultiError) as err:
                logger.exception("Actor crashed:")
                await _debug._maybe_enter_pm(err)

                raise
            finally:
                logger.info("Shutting down root actor")
                await actor.cancel()
    finally:
        _state._current_actor = None
        logger.info("Root actor terminated")


def run(

    # target
    async_fn: typing.Callable[..., typing.Awaitable],
    *args,

    # runtime kwargs
    name: Optional[str] = 'root',
    arbiter_addr: Tuple[str, int] = (
        _default_arbiter_host,
        _default_arbiter_port,
    ),

    start_method: Optional[str] = None,
    debug_mode: bool = False,
    **kwargs,

) -> Any:
    """Run a trio-actor async function in process.

    This is tractor's main entry and the start point for any async actor.
    """
    async def _main():

        async with open_root_actor(
            arbiter_addr=arbiter_addr,
            name=name,
            start_method=start_method,
            debug_mode=debug_mode,
            **kwargs,
        ):

            return await async_fn(*args)

    warnings.warn(
        "`tractor.run()` is now deprecated. `tractor` now"
        " implicitly starts the root actor on first actor nursery"
        " use. If you want to start the root actor manually, use"
        " `tractor.open_root_actor()`.",
        DeprecationWarning,
        stacklevel=2,
    )
    return trio.run(_main)


def run_daemon(
    enable_modules: List[str],
    **kwargs
) -> None:
    """Spawn daemon actor which will respond to RPC.

    This is a convenience wrapper around
    ``tractor.run(trio.sleep(float('inf')))`` such that the first actor spawned
    is meant to run forever responding to RPC requests.
    """
    kwargs['enable_modules'] = list(enable_modules)

    for path in enable_modules:
        importlib.import_module(path)

    return run(partial(trio.sleep, float('inf')), **kwargs)

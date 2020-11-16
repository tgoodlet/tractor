"""
Sub-process entry points.
"""
from functools import partial
from typing import Tuple, Any
import signal

import trio  # type: ignore

from .log import get_console_log, get_logger
from . import _state


log = get_logger(__name__)


def _mp_main(
    actor: 'Actor',  # type: ignore
    accept_addr: Tuple[str, int],
    forkserver_info: Tuple[Any, Any, Any, Any, Any],
    start_method: str,
    parent_addr: Tuple[str, int] = None,
) -> None:
    """The routine called *after fork* which invokes a fresh ``trio.run``
    """
    actor._forkserver_info = forkserver_info
    from ._spawn import try_set_start_method
    spawn_ctx = try_set_start_method(start_method)

    if actor.loglevel is not None:
        log.info(
            f"Setting loglevel for {actor.uid} to {actor.loglevel}")
        get_console_log(actor.loglevel)

    assert spawn_ctx
    log.info(
        f"Started new {spawn_ctx.current_process()} for {actor.uid}")

    _state._current_actor = actor

    log.debug(f"parent_addr is {parent_addr}")
    trio_main = partial(
        actor._async_main,
        accept_addr,
        parent_addr=parent_addr
    )
    try:
        trio.run(trio_main)
    except KeyboardInterrupt:
        pass  # handle it the same way trio does?

    finally:
        log.info(f"Actor {actor.uid} terminated")


def _trio_main(
    actor: 'Actor',  # type: ignore
    *,
    parent_addr: Tuple[str, int] = None,
) -> None:
    """Entry point for a `trio_run_in_process` subactor.
    """
    # Disable sigint handling in children;
    # we don't need it thanks to our cancellation machinery.
    # signal.signal(signal.SIGINT, signal.SIG_IGN)

    log.info(f"Started new trio process for {actor.uid}")

    if actor.loglevel is not None:
        log.info(
            f"Setting loglevel for {actor.uid} to {actor.loglevel}")
        get_console_log(actor.loglevel)

    log.info(
        f"Started {actor.uid}")

    _state._current_actor = actor

    log.debug(f"parent_addr is {parent_addr}")
    trio_main = partial(
        actor._async_main,
        parent_addr=parent_addr
    )

    try:
        trio.run(trio_main)
    except KeyboardInterrupt:
        log.warning(f"Actor {actor.uid} received KBI")

    finally:
        log.info(f"Actor {actor.uid} terminated")

"""
Infection apis for ``asyncio`` loops running ``trio`` using guest mode.
"""
import asyncio
import inspect
from typing import (
    Any,
    Callable,
    AsyncGenerator,
    Awaitable,
    Union,
)

import trio

from .log import get_logger
from ._state import current_actor

log = get_logger(__name__)


__all__ = ['run_task', 'run_as_asyncio_guest']


async def run_coro(
    to_trio: trio.MemorySendChannel,
    coro: Awaitable,
) -> None:
    """Await ``coro`` and relay result back to ``trio``.
    """
    to_trio.send_nowait(await coro)


async def consume_asyncgen(
    to_trio: trio.MemorySendChannel,
    coro: AsyncGenerator,
) -> None:
    """Stream async generator results back to ``trio``.

    ``from_trio`` might eventually be used here for
    bidirectional streaming.
    """
    async for item in coro:
        to_trio.send_nowait(item)


async def run_task(
    func: Callable,
    *,
    qsize: int = 2**10,
    _treat_as_stream: bool = False,
    **kwargs,
) -> Union[AsyncGenerator, Any]:
    """Run an ``asyncio`` async function or generator in a task, return
    or stream the result back to ``trio``.
    """
    assert current_actor().is_infected_aio()

    # ITC (inter task comms)
    from_trio = asyncio.Queue(qsize)  # type: ignore
    to_trio, from_aio = trio.open_memory_channel(qsize)  # type: ignore

    args = tuple(inspect.getfullargspec(func).args)

    if getattr(func, '_tractor_steam_function', None):
        # the assumption is that the target async routine accepts the
        # send channel then it intends to yield more then one return
        # value otherwise it would just return ;P
        _treat_as_stream = True

    # allow target func to accept/stream results manually by name
    if 'to_trio' in args:
        kwargs['to_trio'] = to_trio
    if 'from_trio' in args:
        kwargs['from_trio'] = from_trio

    coro = func(**kwargs)

    cancel_scope = trio.CancelScope()

    # start the asyncio task we submitted from trio
    # TODO: try out ``anyio`` asyncio based tg here
    if inspect.isawaitable(coro):
        task = asyncio.create_task(run_coro(to_trio, coro))
    elif inspect.isasyncgen(coro):
        task = asyncio.create_task(consume_asyncgen(to_trio, coro))
    else:
        raise TypeError(f"No support for {coro}")

    err = None

    def cancel_trio(task):
        """Cancel the calling ``trio`` task on error.
        """
        nonlocal err
        err = task.exception()
        if err:
            log.exception(f"asyncio task errorred:\n{err}")
        cancel_scope.cancel()

    task.add_done_callback(cancel_trio)

    # asycn gen
    if inspect.isasyncgen(coro) or _treat_as_stream:

        async def stream_results():
            with cancel_scope:
                # stream values upward
                async with from_aio:
                    async for item in from_aio:
                        yield item
            if cancel_scope.cancelled_caught and err:
                raise err

        return stream_results()

    # simple async func
    elif inspect.iscoroutine(coro):

        with cancel_scope:
            # return single value
            return await from_aio.receive()
        if cancel_scope.cancelled_caught and err:
            raise err



def run_as_asyncio_guest(
    trio_main: Callable,
) -> None:
    """Entry for an "infected ``asyncio`` actor".

    Uh, oh. :o

    It looks like your event loop has caught a case of the ``trio``s.

    :()

    Don't worry, we've heard you'll barely notice. You might hallucinate
    a few more propagating errors and feel like your digestion has
    slowed but if anything get's too bad your parents will know about
    it.

    :)
    """
    async def aio_main(trio_main):
        loop = asyncio.get_running_loop()

        trio_done_fut = asyncio.Future()

        def trio_done_callback(main_outcome):
            log.info(f"trio_main finished: {main_outcome!r}")
            trio_done_fut.set_result(main_outcome)

        # start the infection: run trio on the asyncio loop in "guest mode"
        log.info(f"Infecting asyncio process with {trio_main}")
        trio.lowlevel.start_guest_run(
            trio_main,
            run_sync_soon_threadsafe=loop.call_soon_threadsafe,
            done_callback=trio_done_callback,
        )

        (await trio_done_fut).unwrap()

    # might as well if it's installed.
    try:
        import uvloop
        loop = uvloop.new_event_loop()
        asyncio.set_event_loop(loop)
    except ImportError:
        pass

    asyncio.run(aio_main(trio_main))

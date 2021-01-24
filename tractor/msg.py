"""
Messaging pattern APIs and helpers.
"""
import inspect
import typing
from typing import Dict, Any, Set, Callable
from functools import partial
from async_generator import aclosing

import trio
import wrapt

from .log import get_logger
from ._streaming import Context

__all__ = ['pub']

log = get_logger('messaging')


async def fan_out_to_ctxs(
    pub_async_gen_func: typing.Callable,  # it's an async gen ... gd mypy
    topics2ctxs: Dict[str, set],
    packetizer: typing.Callable = None,
) -> None:
    """Request and fan out quotes to each subscribed actor channel.
    """
    def get_topics():
        return tuple(topics2ctxs.keys())

    agen = pub_async_gen_func(get_topics=get_topics)

    async with aclosing(agen) as pub_gen:

        async for published in pub_gen:

            ctx_payloads: Dict[str, Any] = {}

            for topic, data in published.items():
                log.debug(f"publishing {topic, data}")
                # build a new dict packet or invoke provided packetizer
                if packetizer is None:
                    packet = {topic: data}
                else:
                    packet = packetizer(topic, data)
                for ctx in topics2ctxs.get(topic, set()):
                    ctx_payloads.setdefault(ctx, {}).update(packet),

            if not ctx_payloads:
                log.debug(f"Unconsumed values:\n{published}")

            # deliver to each subscriber (fan out)
            if ctx_payloads:
                for ctx, payload in ctx_payloads.items():
                    try:
                        await ctx.send_yield(payload)
                    except (
                        # That's right, anything you can think of...
                        trio.ClosedResourceError, ConnectionResetError,
                        ConnectionRefusedError,
                    ):
                        log.warning(f"{ctx.chan} went down?")
                        for ctx_set in topics2ctxs.values():
                            ctx_set.discard(ctx)

            if not get_topics():
                log.warning(f"No subscribers left for {pub_gen}")
                break


def modify_subs(topics2ctxs, topics, ctx):
    """Absolute symbol subscription list for each quote stream.

    Effectively a symbol subscription api.
    """
    log.info(f"{ctx.chan.uid} changed subscription to {topics}")

    # update map from each symbol to requesting client's chan
    for topic in topics:
        topics2ctxs.setdefault(topic, set()).add(ctx)

    # remove any existing symbol subscriptions if symbol is not
    # found in ``symbols``
    # TODO: this can likely be factored out into the pub-sub api
    for topic in filter(
        lambda topic: topic not in topics, topics2ctxs.copy()
    ):
        ctx_set = topics2ctxs.get(topic)
        ctx_set.discard(ctx)

        if not ctx_set:
            # pop empty sets which will trigger bg quoter task termination
            topics2ctxs.pop(topic)


_pub_state: Dict[str, dict] = {}
_pubtask2lock: Dict[str, trio.StrictFIFOLock] = {}


def pub(
    wrapped: typing.Callable = None,
    *,
    tasks: Set[str] = set(),
    send_on_connect: Any = None,
):
    """Publisher async generator decorator.

    A publisher can be called multiple times from different actors but
    will only spawn a finite set of internal tasks to stream values to
    each caller. The ``tasks: Set[str]`` argument to the decorator
    specifies the names of the mutex set of publisher tasks.  When the
    publisher function is called, an argument ``task_name`` must be
    passed to specify which task (of the set named in ``tasks``) should
    be used. This allows for using the same publisher with different
    input (arguments) without allowing more concurrent tasks then
    necessary.

    Values yielded from the decorated async generator must be
    ``Dict[str, Dict[str, Any]]`` where the fist level key is the topic
    string and determines which subscription the packet will be
    delivered to and the value is a packet ``Dict[str, Any]`` by default
    of the form:

    .. ::python

        {topic: str: value: Any}

    The caller can instead opt to pass a ``packetizer`` callback who's
    return value will be delivered as the published response.

    The decorated async generator function must accept an argument
    :func:`get_topics` which dynamically returns the tuple of current
    subscriber topics:

    .. code:: python

        from tractor.msg import pub

        @pub(tasks={'source_1', 'source_2'})
        async def pub_service(get_topics):
            data = await web_request(endpoints=get_topics())
            for item in data:
                yield data['key'], data


    The publisher must be called passing in the following arguments:
    - ``topics: Set[str]`` the topic sequence or "subscriptions"
    - ``task_name: str`` the task to use (if ``tasks`` was passed)
    - ``ctx: Context`` the tractor context (only needed if calling the
      pub func without a nursery, otherwise this is provided implicitly)
    - packetizer: ``Callable[[str, Any], Any]`` a callback who receives
      the topic and value from the publisher function each ``yield`` such that
      whatever is returned is sent as the published value to subscribers of
      that topic.  By default this is a dict ``{topic: str: value: Any}``.

    As an example, to make a subscriber call the above function:

    .. code:: python

        from functools import partial
        import tractor

        async with tractor.open_nursery() as n:
            portal = n.run_in_actor(
                'publisher',  # actor name
                partial(      # func to execute in it
                    pub_service,
                    topics=('clicks', 'users'),
                    task_name='source1',
                )
            )
            async for value in await portal.result():
                print(f"Subscriber received {value}")


    Here, you don't need to provide the ``ctx`` argument since the
    remote actor provides it automatically to the spawned task. If you
    were to call ``pub_service()`` directly from a wrapping function you
    would need to provide this explicitly.

    Remember you only need this if you need *a finite set of tasks*
    running in a single actor to stream data to an arbitrary number of
    subscribers. If you are ok to have a new task running for every call
    to ``pub_service()`` then probably don't need this.
    """
    global _pubtask2lock

    # handle the decorator not called with () case
    if wrapped is None:
        return partial(pub, tasks=tasks, send_on_connect=send_on_connect)

    task2lock: Dict[str, trio.StrictFIFOLock] = {}

    for name in tasks:
        task2lock[name] = trio.StrictFIFOLock()

    @wrapt.decorator
    async def wrapper(agen, instance, args, kwargs):

        # XXX: this is used to extract arguments properly as per the
        # `wrapt` docs
        async def _execute(
            ctx: Context,
            topics: Set[str],
            *args,
            # *,
            task_name: str = None,  # default: only one task allocated
            packetizer: Callable = None,
            **kwargs,
        ):
            if task_name is None:
                task_name = trio.lowlevel.current_task().name

            if tasks and task_name not in tasks:
                raise TypeError(
                    f"{agen} must be called with a `task_name` named "
                    f"argument with a value from {tasks}")

            elif not tasks and not task2lock:
                # add a default root-task lock if none defined
                task2lock[task_name] = trio.StrictFIFOLock()

            _pubtask2lock.update(task2lock)

            topics = set(topics)
            lock = _pubtask2lock[task_name]

            all_subs = _pub_state.setdefault('_subs', {})
            topics2ctxs = all_subs.setdefault(task_name, {})

            try:
                modify_subs(topics2ctxs, topics, ctx)

                # if specified send the startup message back to consumer
                if send_on_connect is not None:
                    await ctx.send_yield(send_on_connect)

                # block and let existing feed task deliver
                # stream data until it is cancelled in which case
                # the next waiting task will take over and spawn it again
                async with lock:
                    # no data feeder task yet; so start one
                    respawn = True
                    while respawn:
                        respawn = False
                        log.info(
                            f"Spawning data feed task for {funcname}")
                        try:
                            # unblocks when no more symbols subscriptions exist
                            # and the streamer task terminates
                            await fan_out_to_ctxs(
                                pub_async_gen_func=partial(
                                    agen, *args, **kwargs),
                                topics2ctxs=topics2ctxs,
                                packetizer=packetizer,
                            )
                            log.info(
                                f"Terminating stream task {task_name or ''}"
                                f" for {agen.__name__}")
                        except trio.BrokenResourceError:
                            log.exception("Respawning failed data feed task")
                            respawn = True
            finally:
                # remove all subs for this context
                modify_subs(topics2ctxs, (), ctx)

                # if there are truly no more subscriptions with this broker
                # drop from broker subs dict
                if not any(topics2ctxs.values()):
                    log.info(
                        f"No more subscriptions for publisher {task_name}")

        # invoke it
        await _execute(*args, **kwargs)

    funcname = wrapped.__name__
    if not inspect.isasyncgenfunction(wrapped):
        raise TypeError(
            f"Publisher {funcname} must be an async generator function"
        )
    if 'get_topics' not in inspect.signature(wrapped).parameters:
        raise TypeError(
            f"Publisher async gen {funcname} must define a "
            "`get_topics` argument"
        )

    # XXX: manually monkey the wrapped function since
    # ``wrapt.decorator`` doesn't seem to want to play nice with its
    # whole "adapter" thing...
    wrapped._tractor_stream_function = True  # type: ignore

    return wrapper(wrapped)

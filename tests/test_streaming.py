"""
Streaming via async gen api
"""
import time
from functools import partial
import platform

import trio
import tractor
from tractor.testing import tractor_test
import pytest


def test_must_define_ctx():

    with pytest.raises(TypeError) as err:
        @tractor.stream
        async def no_ctx():
            pass

    assert "no_ctx must be `ctx: tractor.Context" in str(err.value)

    @tractor.stream
    async def has_ctx(ctx):
        pass


async def async_gen_stream(sequence):
    for i in sequence:
        yield i
        await trio.sleep(0.1)

    # block indefinitely waiting to be cancelled by ``aclose()`` call
    with trio.CancelScope() as cs:
        await trio.sleep(float('inf'))
        assert 0
    assert cs.cancelled_caught


@tractor.stream
async def context_stream(ctx, sequence):
    for i in sequence:
        await ctx.send_yield(i)
        await trio.sleep(0.1)

    # block indefinitely waiting to be cancelled by ``aclose()`` call
    with trio.CancelScope() as cs:
        await trio.sleep(float('inf'))
        assert 0
    assert cs.cancelled_caught


async def stream_from_single_subactor(
    arb_addr,
    start_method,
    stream_func,
):
    """Verify we can spawn a daemon actor and retrieve streamed data.
    """
    # only one per host address, spawns an actor if None

    async with tractor.open_nursery(
        arbiter_addr=arb_addr,
        start_method=start_method,
    ) as nursery:

        async with tractor.find_actor('streamerd') as portals:

            if not portals:

                # no brokerd actor found
                portal = await nursery.start_actor(
                    'streamerd',
                    enable_modules=[__name__],
                )

                seq = range(10)

                async with portal.open_stream_from(
                    stream_func,
                    sequence=list(seq),  # has to be msgpack serializable
                ) as stream:

                    # it'd sure be nice to have an asyncitertools here...
                    iseq = iter(seq)
                    ival = next(iseq)

                    async for val in stream:
                        assert val == ival

                        try:
                            ival = next(iseq)
                        except StopIteration:
                            # should cancel far end task which will be
                            # caught and no error is raised
                            await stream.aclose()

                    await trio.sleep(0.3)

                    try:
                        await stream.__anext__()
                    except StopAsyncIteration:
                        # stop all spawned subactors
                        await portal.cancel_actor()
                    # await nursery.cancel()


@pytest.mark.parametrize(
    'stream_func', [async_gen_stream, context_stream]
)
def test_stream_from_single_subactor(arb_addr, start_method, stream_func):
    """Verify streaming from a spawned async generator.
    """
    trio.run(
        partial(
            stream_from_single_subactor,
            arb_addr,
            start_method,
            stream_func=stream_func,
        ),
    )


# this is the first 2 actors, streamer_1 and streamer_2
async def stream_data(seed):

    for i in range(seed):

        yield i

        # trigger scheduler to simulate practical usage
        await trio.sleep(0)


# this is the third actor; the aggregator
async def aggregate(seed):
    """Ensure that the two streams we receive match but only stream
    a single set of values to the parent.
    """
    async with tractor.open_nursery() as nursery:
        portals = []
        for i in range(1, 3):
            # fork point
            portal = await nursery.start_actor(
                name=f'streamer_{i}',
                enable_modules=[__name__],
            )

            portals.append(portal)

        send_chan, recv_chan = trio.open_memory_channel(500)

        async def push_to_chan(portal, send_chan):
            async with send_chan:

                async with portal.open_stream_from(
                    stream_data, seed=seed,
                ) as stream:

                    async for value in stream:
                        # leverage trio's built-in backpressure
                        await send_chan.send(value)

            print(f"FINISHED ITERATING {portal.channel.uid}")

        # spawn 2 trio tasks to collect streams and push to a local queue
        async with trio.open_nursery() as n:

            for portal in portals:
                n.start_soon(push_to_chan, portal, send_chan.clone())

            # close this local task's reference to send side
            await send_chan.aclose()

            unique_vals = set()
            async with recv_chan:
                async for value in recv_chan:
                    if value not in unique_vals:
                        unique_vals.add(value)
                        # yield upwards to the spawning parent actor
                        yield value

                assert value in unique_vals

            print("FINISHED ITERATING in aggregator")

        await nursery.cancel()
        print("WAITING on `ActorNursery` to finish")
    print("AGGREGATOR COMPLETE!")


# this is the main actor and *arbiter*
async def a_quadruple_example():
    # a nursery which spawns "actors"
    async with tractor.open_nursery() as nursery:

        seed = int(1e3)
        pre_start = time.time()

        portal = await nursery.start_actor(
            name='aggregator',
            enable_modules=[__name__],
        )

        start = time.time()
        # the portal call returns exactly what you'd expect
        # as if the remote "aggregate" function was called locally
        result_stream = []

        async with portal.open_stream_from(aggregate, seed=seed) as stream:
            async for value in stream:
                result_stream.append(value)

        print(f"STREAM TIME = {time.time() - start}")
        print(f"STREAM + SPAWN TIME = {time.time() - pre_start}")
        assert result_stream == list(range(seed))
        await portal.cancel_actor()
        return result_stream


async def cancel_after(wait, arb_addr):
    async with tractor.open_root_actor(arbiter_addr=arb_addr):
        with trio.move_on_after(wait):
            return await a_quadruple_example()


@pytest.fixture(scope='module')
def time_quad_ex(arb_addr, ci_env, spawn_backend):
    if spawn_backend == 'mp':
        """no idea but the  mp *nix runs are flaking out here often...
        """
        pytest.skip("Test is too flaky on mp in CI")

    timeout = 7 if platform.system() in ('Windows', 'Darwin') else 4
    start = time.time()
    results = trio.run(cancel_after, timeout, arb_addr)
    diff = time.time() - start
    assert results
    return results, diff


def test_a_quadruple_example(time_quad_ex, ci_env, spawn_backend):
    """This also serves as a kind of "we'd like to be this fast test"."""

    results, diff = time_quad_ex
    assert results
    this_fast = 6 if platform.system() in ('Windows', 'Darwin') else 2.5
    assert diff < this_fast


@pytest.mark.parametrize(
    'cancel_delay',
    list(map(lambda i: i/10, range(3, 9)))
)
def test_not_fast_enough_quad(
    arb_addr, time_quad_ex, cancel_delay, ci_env, spawn_backend
):
    """Verify we can cancel midway through the quad example and all actors
    cancel gracefully.
    """
    results, diff = time_quad_ex
    delay = max(diff - cancel_delay, 0)
    results = trio.run(cancel_after, delay, arb_addr)
    system = platform.system()
    if system in ('Windows', 'Darwin') and results is not None:
        # In CI envoirments it seems later runs are quicker then the first
        # so just ignore these
        print(f"Woa there {system} caught your breath eh?")
    else:
        # should be cancelled mid-streaming
        assert results is None


@tractor_test
async def test_respawn_consumer_task(
    arb_addr,
    spawn_backend,
    loglevel,
):
    """Verify that ``._portal.ReceiveStream.shield()``
    sucessfully protects the underlying IPC channel from being closed
    when cancelling and respawning a consumer task.

    This also serves to verify that all values from the stream can be
    received despite the respawns.

    """
    stream = None

    async with tractor.open_nursery() as n:

        portal = await n.start_actor(
            name='streamer',
            enable_modules=[__name__]
        )
        async with portal.open_stream_from(
            stream_data,
            seed=11,
        ) as stream:

            expect = set(range(11))
            received = []

            # this is the re-spawn task routine
            async def consume(task_status=trio.TASK_STATUS_IGNORED):
                print('starting consume task..')
                nonlocal stream

                with trio.CancelScope() as cs:
                    task_status.started(cs)

                    # shield stream's underlying channel from cancellation
                    with stream.shield():

                        async for v in stream:
                            print(f'from stream: {v}')
                            expect.remove(v)
                            received.append(v)

                    print('exited consume')

            async with trio.open_nursery() as ln:
                cs = await ln.start(consume)

                while True:

                    await trio.sleep(0.1)

                    if received[-1] % 2 == 0:

                        print('cancelling consume task..')
                        cs.cancel()

                        # respawn
                        cs = await ln.start(consume)

                    if not expect:
                        print("all values streamed, BREAKING")
                        break

                cs.cancel()

        # TODO: this is justification for a
        # ``ActorNursery.stream_from_actor()`` helper?
        await portal.cancel_actor()

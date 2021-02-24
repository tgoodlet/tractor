import time
from itertools import cycle

import pytest
import trio
import tractor
from tractor.testing import tractor_test


def test_type_checks():

    with pytest.raises(TypeError) as err:
        @tractor.msg.pub
        async def no_get_topics(yo):
            yield

    assert "must define a `get_topics`" in str(err.value)

    with pytest.raises(TypeError) as err:
        @tractor.msg.pub
        def not_async_gen(yo):
            pass

    assert "must be an async generator function" in str(err.value)


def is_even(i):
    return i % 2 == 0


# placeholder for topics getter
_get_topics = None


@tractor.msg.pub
async def pubber(get_topics, seed=10):

    # ensure topic subscriptions are as expected
    global _get_topics
    _get_topics = get_topics

    for i in cycle(range(seed)):

        yield {'even' if is_even(i) else 'odd': i}
        await trio.sleep(0.1)


async def subs(
    which,
    pub_actor_name,
    seed=10,
    task_status=trio.TASK_STATUS_IGNORED,
):
    if len(which) == 1:
        if which[0] == 'even':
            pred = is_even
        else:
            def pred(i):
                return not is_even(i)
    else:
        def pred(i):
            return isinstance(i, int)

    # TODO: https://github.com/goodboy/tractor/issues/207
    async with tractor.wait_for_actor(pub_actor_name) as portal:
        assert portal

        async with portal.open_stream_from(
            pubber,
            topics=which,
            seed=seed,
        ) as stream:
            task_status.started(stream)
            times = 10
            count = 0
            await stream.__anext__()
            async for pkt in stream:
                for topic, value in pkt.items():
                    assert pred(value)
                count += 1
                if count >= times:
                    break

            await stream.aclose()

        async with portal.open_stream_from(
            pubber,
            topics=['odd'],
            seed=seed,
        ) as stream:
            await stream.__anext__()
            count = 0
            # async with aclosing(stream) as stream:
            try:
                async for pkt in stream:
                    for topic, value in pkt.items():
                        pass
                        # assert pred(value)
                    count += 1
                    if count >= times:
                        break
            finally:
                await stream.aclose()


@tractor.msg.pub(tasks=['one', 'two'])
async def multilock_pubber(get_topics):
    yield {'doggy': 10}


@pytest.mark.parametrize(
    'callwith_expecterror',
    [
        (pubber, {}, TypeError),
        # missing a `topics`
        (multilock_pubber, {'ctx': None}, TypeError),
        # missing a `task_name`
        (multilock_pubber, {'ctx': None, 'topics': ['topic1']}, TypeError),
        # should work
        (multilock_pubber,
         {'ctx': None, 'topics': ['doggy'], 'task_name': 'one'},
         None),
    ],
)
@tractor_test
async def test_required_args(callwith_expecterror):
    func, kwargs, err = callwith_expecterror

    if err is not None:
        with pytest.raises(err):
            await func(**kwargs)
    else:
        async with tractor.open_nursery() as n:

            portal = await n.start_actor(
                name='pubber',
                enable_modules=[__name__],
            )

            async with tractor.wait_for_actor('pubber'):
                pass

            await trio.sleep(0.5)

            async with portal.open_stream_from(
                multilock_pubber,
                **kwargs
            ) as stream:
                async for val in stream:
                    assert val == {'doggy': 10}

            await portal.cancel_actor()


@pytest.mark.parametrize(
    'pub_actor',
    ['streamer', 'arbiter']
)
def test_multi_actor_subs_arbiter_pub(
    loglevel,
    arb_addr,
    pub_actor,
):
    """Try out the neato @pub decorator system.
    """
    global _get_topics

    async def main():

        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
            enable_modules=[__name__],
            debug_mode=True,
        ) as n:

            name = 'root'

            if pub_actor == 'streamer':
                # start the publisher as a daemon
                master_portal = await n.start_actor(
                    'streamer',
                    enable_modules=[__name__],
                )

            even_portal = await n.run_in_actor(
                subs,
                which=['even'],
                name='evens',
                pub_actor_name=name
            )
            odd_portal = await n.run_in_actor(
                subs,
                which=['odd'],
                name='odds',
                pub_actor_name=name
            )

            async with tractor.wait_for_actor('evens'):
                # block until 2nd actor is initialized
                pass

            if pub_actor == 'arbiter':

                # wait for publisher task to be spawned in a local RPC task
                while _get_topics is None:
                    await trio.sleep(0.1)

                get_topics = _get_topics

                assert 'even' in get_topics()

            async with tractor.wait_for_actor('odds'):
                # block until 2nd actor is initialized
                pass

            if pub_actor == 'arbiter':
                start = time.time()
                while 'odd' not in get_topics():
                    await trio.sleep(0.1)
                    if time.time() - start > 1:
                        pytest.fail("odds subscription never arrived?")

            # TODO: how to make this work when the arbiter gets
            # a portal to itself? Currently this causes a hang
            # when the channel server is torn down due to a lingering
            # loopback channel
            #     with trio.move_on_after(1):
            #         await subs(['even', 'odd'])

            # XXX: this would cause infinite
            # blocking due to actor never terminating loop
            # await even_portal.result()

            await trio.sleep(0.5)
            await even_portal.cancel_actor()
            await trio.sleep(1)

            if pub_actor == 'arbiter':
                assert 'even' not in get_topics()

            await odd_portal.cancel_actor()
            await trio.sleep(2)

            if pub_actor == 'arbiter':
                while get_topics():
                    await trio.sleep(0.1)
                    if time.time() - start > 2:
                        pytest.fail("odds subscription never dropped?")
            else:
                await master_portal.cancel_actor()

    trio.run(main)


def test_single_subactor_pub_multitask_subs(
    loglevel,
    arb_addr,
):
    async def main():

        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
            enable_modules=[__name__],
        ) as n:

            portal = await n.start_actor(
                'streamer',
                enable_modules=[__name__],
            )
            async with tractor.wait_for_actor('streamer'):
                # block until 2nd actor is initialized
                pass

            async with trio.open_nursery() as tn:
                agen = await tn.start(subs, ['even'], 'streamer')

                await trio.sleep(0.1)
                tn.start_soon(subs, ['even'], 'streamer')

                # XXX this will trigger the python bug:
                # https://bugs.python.org/issue32526
                # if using async generators to wrap tractor channels
                await agen.aclose()

                await trio.sleep(0.1)
                tn.start_soon(subs, ['even'], 'streamer')
                await trio.sleep(0.1)
                tn.start_soon(subs, ['even'], 'streamer')

            await portal.cancel_actor()

    trio.run(main)

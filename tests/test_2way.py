"""
Bidirectional streaming and context API.

"""
import pytest
import trio
import tractor

# from conftest import tractor_test

# TODO: test endofchannel semantics / cancellation / error cases:
# 3 possible outcomes:
# - normal termination: far end relays a stop message with
# final value as in async gen from ``return <val>``.

# possible outcomes:
# - normal termination: far end returns
# - premature close: far end relays a stop message to tear down stream
# - cancel: far end raises `ContextCancelled`

# future possible outcomes
# - restart request: far end raises `ContextRestart`


_state: bool = False


@tractor.context
async def simple_setup_teardown(

    ctx: tractor.Context,
    data: int,

) -> None:

    # startup phase
    global _state
    _state = True

    # signal to parent that we're up
    await ctx.started(data + 1)

    try:
        # block until cancelled
        await trio.sleep_forever()
    finally:
        _state = False


async def assert_state(value: bool):
    global _state
    assert _state == value


def test_simple_contex():

    async def main():
        async with tractor.open_nursery() as n:

            portal = await n.start_actor(
                'simple_context',
                enable_modules=[__name__],
            )

            async with portal.open_context(
                simple_setup_teardown,
                data=10,
            ) as (ctx, sent):

                assert sent == 11

                await portal.run(assert_state, value=True)

            # after cancellation
            await portal.run(assert_state, value=False)

            # shut down daemon
            await portal.cancel_actor()

    trio.run(main)


@tractor.context
async def simple_rpc(

    ctx: tractor.Context,
    data: int,

) -> None:
    """Test a small ping-pong server.

    """
    # signal to parent that we're up
    await ctx.started(data + 1)

    print('opening stream in callee')
    async with ctx.open_stream() as stream:

        count = 0
        while True:
            try:
                await stream.receive() == 'ping'
            except trio.EndOfChannel:
                assert count == 10
                break
            else:
                print('pong')
                await stream.send('pong')
                count += 1


@tractor.context
async def simple_rpc_with_forloop(

    ctx: tractor.Context,
    data: int,

) -> None:
    """Same as previous test but using ``async for`` syntax/api.

    """

    # signal to parent that we're up
    await ctx.started(data + 1)

    print('opening stream in callee')
    async with ctx.open_stream() as stream:

        count = 0
        async for msg in stream:

            assert msg == 'ping'
            print('pong')
            await stream.send('pong')
            count += 1

        else:
            assert count == 10


@pytest.mark.parametrize(
    'use_async_for',
    [True, False],
)
@pytest.mark.parametrize(
    'server_func',
    [simple_rpc, simple_rpc_with_forloop],
)
def test_simple_rpc(server_func, use_async_for):
    """The simplest request response pattern.

    """
    async def main():
        async with tractor.open_nursery() as n:

            portal = await n.start_actor(
                'rpc_server',
                enable_modules=[__name__],
            )

            async with portal.open_context(
                server_func,  # taken from pytest parameterization
                data=10,
            ) as (ctx, sent):

                assert sent == 11

                async with ctx.open_stream() as stream:

                    if use_async_for:

                        count = 0
                        # receive msgs using async for style
                        print('ping')
                        await stream.send('ping')

                        async for msg in stream:
                            assert msg == 'pong'
                            print('ping')
                            await stream.send('ping')
                            count += 1

                            if count >= 9:
                                break

                    else:
                        # classic send/receive style
                        for _ in range(10):

                            print('ping')
                            await stream.send('ping')
                            assert await stream.receive() == 'pong'

                # stream should terminate here

            await portal.cancel_actor()

    trio.run(main)

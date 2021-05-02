"""
Bidirectional streaming and context API.
"""

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


def test_simple_rpc():
    """The simplest request response pattern.

    """
    async def main():
        async with tractor.open_nursery() as n:

            portal = await n.start_actor(
                'rpc_server',
                enable_modules=[__name__],
            )

            async with portal.open_context(
                simple_rpc,
                data=10,
            ) as (ctx, sent):

                assert sent == 11

                async with ctx.open_stream() as stream:

                    for _ in range(10):

                        print('ping')
                        await stream.send('ping')
                        assert await stream.receive() == 'pong'

                # stream should terminate here

            await portal.cancel_actor()

    trio.run(main)

from itertools import repeat
import trio
import tractor

tractor.log.get_console_log("INFO")


async def stream_forever():
    for i in repeat("I can see these little future bubble things"):
        # each yielded value is sent over the ``Channel`` to the
        # parent actor
        yield i
        await trio.sleep(0.01)


async def main():

    # stream for at most 1 seconds
    with trio.move_on_after(1) as cancel_scope:

        async with tractor.open_nursery() as n:

            portal = await n.start_actor(
                'donny',
                enable_modules=[__name__],
            )

            # this async for loop streams values from the above
            # async generator running in a separate process
            async with portal.open_stream_from(stream_forever) as stream:
                async for letter in stream:
                    print(letter)

    # we support trio's cancellation system
    assert cancel_scope.cancelled_caught
    assert n.cancelled


if __name__ == '__main__':
    trio.run(main)

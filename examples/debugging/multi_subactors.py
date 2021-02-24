import tractor
import trio


async def breakpoint_forever():
    "Indefinitely re-enter debugger in child actor."
    while True:
        await trio.sleep(0.1)
        await tractor.breakpoint()


async def name_error():
    "Raise a ``NameError``"
    getattr(doggypants)  # noqa


async def spawn_error():
    """"A nested nursery that triggers another ``NameError``.
    """
    async with tractor.open_nursery() as n:
        portal = await n.run_in_actor(
            name_error,
            name='name_error_1',
        )
        return await portal.result()


async def main():
    """The main ``tractor`` routine.

    The process tree should look as approximately as follows:

    -python examples/debugging/multi_subactors.py
    |-python -m tractor._child --uid ('name_error', 'a7caf490 ...)
    |-python -m tractor._child --uid ('bp_forever', '1f787a7e ...)
    `-python -m tractor._child --uid ('spawn_error', '52ee14a5 ...)
       `-python -m tractor._child --uid ('name_error', '3391222c ...)
    """
    async with tractor.open_nursery(
        debug_mode=True,
    ) as n:

        # Spawn both actors, don't bother with collecting results
        # (would result in a different debugger outcome due to parent's
        # cancellation).
        await n.run_in_actor(breakpoint_forever)
        await n.run_in_actor(name_error)
        await n.run_in_actor(spawn_error)


if __name__ == '__main__':
    trio.run(main)

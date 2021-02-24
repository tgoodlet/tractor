import trio
import tractor


async def name_error():
    "Raise a ``NameError``"
    getattr(doggypants)  # noqa


async def spawn_until(depth=0):
    """"A nested nursery that triggers another ``NameError``.
    """
    async with tractor.open_nursery() as n:
        if depth < 1:
            # await n.run_in_actor('breakpoint_forever', breakpoint_forever)
            await n.run_in_actor(name_error)
        else:
            depth -= 1
            await n.run_in_actor(
                spawn_until,
                depth=depth,
                name=f'spawn_until_{depth}',
            )


async def main():
    """The main ``tractor`` routine.

    The process tree should look as approximately as follows when the debugger
    first engages:

    python examples/debugging/multi_nested_subactors_bp_forever.py
    ├─ python -m tractor._child --uid ('spawner1', '7eab8462 ...)
    │  └─ python -m tractor._child --uid ('spawn_until_0', '3720602b ...)
    │     └─ python -m tractor._child --uid ('name_error', '505bf71d ...)
    │
    └─ python -m tractor._child --uid ('spawner0', '1d42012b ...)
       └─ python -m tractor._child --uid ('name_error', '6c2733b8 ...)

    """
    async with tractor.open_nursery(
        debug_mode=True,
        loglevel='warning'
    ) as n:

        # spawn both actors
        portal = await n.run_in_actor(
            spawn_until,
            depth=0,
            name='spawner0',
        )
        portal1 = await n.run_in_actor(
            spawn_until,
            depth=1,
            name='spawner1',
        )

        # nursery cancellation should be triggered due to propagated
        # error from child.
        await portal.result()
        await portal1.result()


if __name__ == '__main__':
    trio.run(main)

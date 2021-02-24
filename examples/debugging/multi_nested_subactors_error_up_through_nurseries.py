import trio
import tractor


async def name_error():
    "Raise a ``NameError``"
    getattr(doggypants)  # noqa


async def breakpoint_forever():
    "Indefinitely re-enter debugger in child actor."
    while True:
        await tractor.breakpoint()


async def spawn_until(depth=0):
    """"A nested nursery that triggers another ``NameError``.
    """
    async with tractor.open_nursery() as n:
        if depth < 1:
            # await n.run_in_actor('breakpoint_forever', breakpoint_forever)
            await n.run_in_actor(
                name_error,
                name='name_error'
            )
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
    │  └─ python -m tractor._child --uid ('spawn_until_3', 'afcba7a8 ...)
    │     └─ python -m tractor._child --uid ('spawn_until_2', 'd2433d13 ...)
    │        └─ python -m tractor._child --uid ('spawn_until_1', '1df589de ...)
    │           └─ python -m tractor._child --uid ('spawn_until_0', '3720602b ...)
    │
    └─ python -m tractor._child --uid ('spawner0', '1d42012b ...)
       └─ python -m tractor._child --uid ('spawn_until_2', '2877e155 ...)
          └─ python -m tractor._child --uid ('spawn_until_1', '0502d786 ...)
             └─ python -m tractor._child --uid ('spawn_until_0', 'de918e6d ...)

    """
    async with tractor.open_nursery(
        debug_mode=True,
    ) as n:

        # spawn both actors
        portal = await n.run_in_actor(
            spawn_until,
            depth=3,
            name='spawner0',
        )
        portal1 = await n.run_in_actor(
            spawn_until,
            depth=4,
            name='spawner1',
        )

        # gah still an issue here.
        await portal.result()
        await portal1.result()


if __name__ == '__main__':
    trio.run(main)

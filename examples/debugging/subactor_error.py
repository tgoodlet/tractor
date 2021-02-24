import trio
import tractor


async def name_error():
    getattr(doggypants)


async def main():
    async with tractor.open_nursery(
        debug_mode=True,
    ) as n:

        portal = await n.run_in_actor(name_error)
        await portal.result()


if __name__ == '__main__':
    trio.run(main)

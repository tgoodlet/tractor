import trio
import tractor


async def main():

    async with tractor.open_root_actor(
        debug_mode=True,
    ):

        await trio.sleep(0.1)

        await tractor.breakpoint()

        await trio.sleep(0.1)


if __name__ == '__main__':
    trio.run(main)

import trio
import tractor


async def main():

    async with tractor.open_root_actor(
        debug_mode=True,
    ):
        while True:
            await tractor.breakpoint()


if __name__ == '__main__':
    trio.run(main)

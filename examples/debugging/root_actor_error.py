import trio
import tractor


async def main():
    async with tractor.open_root_actor(
        debug_mode=True,
    ):
        assert 0


if __name__ == '__main__':
    trio.run(main)

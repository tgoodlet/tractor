import trio
import tractor


async def assert_err():
    assert 0


async def main():
    async with tractor.open_nursery() as n:
        real_actors = []
        for i in range(3):
            real_actors.append(await n.start_actor(
                f'actor_{i}',
                enable_modules=[__name__],
            ))

        # start one actor that will fail immediately
        await n.run_in_actor(assert_err)

    # should error here with a ``RemoteActorError`` containing
    # an ``AssertionError`` and all the other actors have been cancelled


if __name__ == '__main__':
    try:
        # also raises
        trio.run(main)
    except tractor.RemoteActorError:
        print("Look Maa that actor failed hard, hehhh!")

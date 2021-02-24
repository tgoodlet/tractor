import trio
import tractor


async def movie_theatre_question():
    """A question asked in a dark theatre, in a tangent
    (errr, I mean different) process.
    """
    return 'have you ever seen a portal?'


async def main():
    """The main ``tractor`` routine.
    """
    async with tractor.open_nursery() as n:

        portal = await n.start_actor(
            'frank',
            # enable the actor to run funcs from this current module
            rpc_module_paths=[__name__],
        )

        print(await portal.run(movie_theatre_question))
        # call the subactor a 2nd time
        print(await portal.run(movie_theatre_question))

        # the async with will block here indefinitely waiting
        # for our actor "frank" to complete, but since it's an
        # "outlive_main" actor it will never end until cancelled
        await portal.cancel_actor()


if __name__ == '__main__':
    trio.run(main)

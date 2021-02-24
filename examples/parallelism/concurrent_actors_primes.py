"""
Demonstration of the prime number detector example from the
``concurrent.futures`` docs:

https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor-example

This uses no extra threads, fancy semaphores or futures; all we need
is ``tractor``'s channels.

"""
from contextlib import asynccontextmanager
from typing import List, Callable
import itertools
import math
import time

import tractor
import trio
from async_generator import aclosing


PRIMES = [
    112272535095293,
    112582705942171,
    112272535095293,
    115280095190773,
    115797848077099,
    1099726899285419,
]


async def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False

    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False
    return True


@asynccontextmanager
async def worker_pool(workers=4):
    """Though it's a trivial special case for ``tractor``, the well
    known "worker pool" seems to be the defacto "but, I want this
    process pattern!" for most parallelism pilgrims.

    Yes, the workers stay alive (and ready for work) until you close
    the context.
    """
    async with tractor.open_nursery() as tn:

        portals = []
        snd_chan, recv_chan = trio.open_memory_channel(len(PRIMES))

        for i in range(workers):

            # this starts a new sub-actor (process + trio runtime) and
            # stores it's "portal" for later use to "submit jobs" (ugh).
            portals.append(
                await tn.start_actor(
                    f'worker_{i}',
                    enable_modules=[__name__],
                )
            )

        async def _map(
            worker_func: Callable[[int], bool],
            sequence: List[int]
        ) -> List[bool]:

            # define an async (local) task to collect results from workers
            async def send_result(func, value, portal):
                await snd_chan.send((value, await portal.run(func, n=value)))

            async with trio.open_nursery() as n:

                for value, portal in zip(sequence, itertools.cycle(portals)):
                    n.start_soon(
                        send_result,
                        worker_func,
                        value,
                        portal
                    )

                # deliver results as they arrive
                for _ in range(len(sequence)):
                    yield await recv_chan.receive()

        # deliver the parallel "worker mapper" to user code
        yield _map

        # tear down all "workers" on pool close
        await tn.cancel()


async def main():

    async with worker_pool() as actor_map:

        start = time.time()

        async with aclosing(actor_map(is_prime, PRIMES)) as results:
            async for number, prime in results:

                print(f'{number} is prime: {prime}')

        print(f'processing took {time.time() - start} seconds')


if __name__ == '__main__':
    start = time.time()
    trio.run(main)
    print(f'script took {time.time() - start} seconds')

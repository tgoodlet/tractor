|logo| ``tractor``: next-gen Python parallelism

|gh_actions|
|docs|

.. _actor model: https://en.wikipedia.org/wiki/Actor_model
.. _trio: https://github.com/python-trio/trio
.. _multi-processing: https://en.wikipedia.org/wiki/Multiprocessing
.. _trionic: https://trio.readthedocs.io/en/latest/design.html#high-level-design-principles
.. _async sandwich: https://trio.readthedocs.io/en/latest/tutorial.html#async-sandwich
.. _structured concurrent: https://trio.discourse.group/t/concise-definition-of-structured-concurrency/228


``tractor`` is a `structured concurrent`_ "`actor model`_" built on trio_ and multi-processing_.

We pair `structured concurrency`_ and true multi-core parallelism with
the aim of being the multi-processing framework *you always wanted*.

The first step to grok ``tractor`` is to get the basics of ``trio`` down.
A great place to start is the `trio docs`_ and this `blog post`_.


Features
--------
- **It's just** a ``trio`` API
- Infinitely nesteable process trees
- Built-in APIs for inter-process streaming
- A (first ever?) "native" multi-core debugger for Python using `pdb++`_
- Support for multiple process spawning backends
- A modular transport layer, allowing for custom serialization,
  communications protocols, and environment specific IPC primitives


Run a func in a process
-----------------------
Use ``trio``'s style of focussing on *tasks as functions*:

.. code:: python

    """
    Run with a process monitor from a terminal using::

        $TERM -e watch -n 0.1  "pstree -a $$" \
            & python examples/parallelism/single_func.py \
            && kill $!

    """
    import os

    import tractor
    import trio


    async def burn_cpu():

        pid = os.getpid()

        # burn a core @ ~ 50kHz
        for _ in range(50000):
            await trio.sleep(1/50000/50)

        return os.getpid()


    async def main():

        async with tractor.open_nursery() as n:

            portal = await n.run_in_actor(burn_cpu)

            #  burn rubber in the parent too
            await burn_cpu()

            # wait on result from target function
            pid = await portal.result()

        # end of nursery block
        print(f"Collected subproc {pid}")


    if __name__ == '__main__':
        trio.run(main)


This runs ``burn_cpu()`` in a new process and reaps it on completion
of the nursery block.

If you only need to run a sync function and retreive a single result, you
might want to check out `trio-parallel`_.


Zombie safe: self-destruct a process tree
-----------------------------------------
``tractor`` tries to protect you from zombies, no matter what.

.. code:: python

    """
    Run with a process monitor from a terminal using::

        $TERM -e watch -n 0.1  "pstree -a $$" \
            & python examples/parallelism/we_are_processes.py \
            && kill $!

    """
    from multiprocessing import cpu_count
    import os

    import tractor
    import trio


    async def target():
        print(
            f"Yo, i'm '{tractor.current_actor().name}' "
            f"running in pid {os.getpid()}"
        )

       await trio.sleep_forever()


    async def main():

        async with tractor.open_nursery() as n:

            for i in range(cpu_count()):
                await n.run_in_actor(target, name=f'worker_{i}')

            print('This process tree will self-destruct in 1 sec...')
            await trio.sleep(1)

            # you could have done this yourself
            raise Exception('Self Destructed')


    if __name__ == '__main__':
        try:
            trio.run(main)
        except Exception:
            print('Zombies Contained')


If you can create zombie child processes (without using a system signal)
it **is a bug**.


"Native" multi-process debugging
--------------------------------
Using the magic of `pdb++`_ and our internal IPC, we've
been able to create a native feeling debugging experience for
any (sub-)process in your ``tractor`` tree.

.. code:: python

    from os import getpid

    import tractor
    import trio


    async def breakpoint_forever():
        "Indefinitely re-enter debugger in child actor."
        while True:
            yield 'yo'
            await tractor.breakpoint()


    async def name_error():
        "Raise a ``NameError``"
        getattr(doggypants)


    async def main():
        """Test breakpoint in a streaming actor.
        """
        async with tractor.open_nursery(
            debug_mode=True,
            loglevel='error',
        ) as n:

            p0 = await n.start_actor('bp_forever', enable_modules=[__name__])
            p1 = await n.start_actor('name_error', enable_modules=[__name__])

            # retreive results
            stream = await p0.run(breakpoint_forever)
            await p1.run(name_error)


    if __name__ == '__main__':
        trio.run(main)


You can run this with::

    >>> python examples/debugging/multi_daemon_subactors.py

And, yes, there's a built-in crash handling mode B)

We're hoping to add a respawn-from-repl system soon!


Worker poolz are easy peasy
---------------------------
The initial ask from most new users is *"how do I make a worker
pool thing?"*.

``tractor`` is built to handle any SC (structured concurrent) process
tree you can imagine; a "worker pool" pattern is a trivial special
case.

We have a `full worker pool re-implementation`_ of the std-lib's
``concurrent.futures.ProcessPoolExecutor`` example for reference.

You can run it like so (from this dir) to see the process tree in
real time::

    $TERM -e watch -n 0.1  "pstree -a $$" \
        & python examples/parallelism/concurrent_actors_primes.py \
        && kill $!

This uses no extra threads, fancy semaphores or futures; all we need
is ``tractor``'s IPC!


.. _full worker pool re-implementation: https://github.com/goodboy/tractor/blob/master/examples/parallelism/concurrent_actors_primes.py

Install
-------
From PyPi::

    pip install tractor


From git::

    pip install git+git://github.com/goodboy/tractor.git


Under the hood
--------------
``tractor`` is an attempt to pair trionic_ `structured concurrency`_ with
distributed Python. You can think of it as a ``trio``
*-across-processes* or simply as an opinionated replacement for the
stdlib's ``multiprocessing`` but built on async programming primitives
from the ground up.

``tractor``'s nurseries let you spawn ``trio`` *"actors"*: new Python
processes which each run a ``trio`` scheduled runtime - a call to ``trio.run()``.

Don't be scared off by this description. ``tractor`` **is just** ``trio``
but with nurseries for process management and cancel-able streaming IPC.
If you understand how to work with ``trio``, ``tractor`` will give you
the parallelism you've been missing.

"Actors" communicate by exchanging asynchronous messages_ and avoid
sharing state. The intention of this model is to allow for highly
distributed software that, through the adherence to *structured
concurrency*, results in systems which fail in predictable and
recoverable ways.


What's on the TODO:
-------------------
Help us push toward the future.

- (Soon to land) ``asyncio`` support allowing for "infected" actors where
  `trio` drives the `asyncio` scheduler via the astounding "`guest mode`_"
- Typed messaging protocols (ex. via ``msgspec``)
- Erlang-style supervisors via composed context managers


Feel like saying hi?
--------------------
This project is very much coupled to the ongoing development of
``trio`` (i.e. ``tractor`` gets most of its ideas from that brilliant
community). If you want to help, have suggestions or just want to
say hi, please feel free to reach us in our `matrix channel`_.  If
matrix seems too hip, we're also mostly all in the the `trio gitter
channel`_!

.. _trio gitter channel: https://gitter.im/python-trio/general
.. _matrix channel: https://matrix.to/#/!tractor:matrix.org
.. _pdb++: https://github.com/pdbpp/pdbpp
.. _guest mode: https://trio.readthedocs.io/en/stable/reference-lowlevel.html?highlight=guest%20mode#using-guest-mode-to-run-trio-on-top-of-other-event-loops
.. _messages: https://en.wikipedia.org/wiki/Message_passing
.. _trio docs: https://trio.readthedocs.io/en/latest/
.. _blog post: https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
.. _structured concurrency: https://en.wikipedia.org/wiki/Structured_concurrency
.. _3 axioms: https://en.wikipedia.org/wiki/Actor_model#Fundamental_concepts
.. _unrequirements: https://en.wikipedia.org/wiki/Actor_model#Direct_communication_and_asynchrony
.. _async generators: https://www.python.org/dev/peps/pep-0525/
.. _trio-parallel: https://github.com/richardsheridan/trio-parallel


.. |gh_actions| image:: https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fgoodboy%2Ftractor%2Fbadge&style=popout-square
    :target: https://actions-badge.atrox.dev/goodboy/tractor/goto

.. |docs| image:: https://readthedocs.org/projects/tractor/badge/?version=latest
    :target: https://tractor.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. |logo| image:: _static/tractor_logo_side.svg
    :width: 250
    :align: middle

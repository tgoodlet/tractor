.. tractor documentation master file, created by
   sphinx-quickstart on Sun Feb  9 22:26:51 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

tractor
=======
A `structured concurrent`_, async-native "`actor model`_" built on trio_ and multiprocessing_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. _actor model: https://en.wikipedia.org/wiki/Actor_model
.. _trio: https://github.com/python-trio/trio
.. _multiprocessing: https://en.wikipedia.org/wiki/Multiprocessing
.. _trionic: https://trio.readthedocs.io/en/latest/design.html#high-level-design-principles
.. _async sandwich: https://trio.readthedocs.io/en/latest/tutorial.html#async-sandwich
.. _structured concurrent: https://trio.discourse.group/t/concise-definition-of-structured-concurrency/228


``tractor`` is an attempt to bring trionic_ `structured concurrency`_ to
distributed multi-core Python; it aims to be the Python multi-processing
framework *you always wanted*.

``tractor`` lets you spawn ``trio`` *"actors"*: processes which each run
a ``trio`` scheduled task tree (also known as an `async sandwich`_).
*Actors* communicate by exchanging asynchronous messages_ and avoid
sharing any state. This model allows for highly distributed software
architecture which works just as well on multiple cores as it does over
many hosts.

The first step to grok ``tractor`` is to get the basics of ``trio`` down.
A great place to start is the `trio docs`_ and this `blog post`_.

.. _messages: https://en.wikipedia.org/wiki/Message_passing
.. _trio docs: https://trio.readthedocs.io/en/latest/
.. _blog post: https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
.. _structured concurrency: https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/


Install
-------
No PyPi release yet!

::

    pip install git+git://github.com/goodboy/tractor.git


Feel like saying hi?
--------------------
This project is very much coupled to the ongoing development of
``trio`` (i.e. ``tractor`` gets all its ideas from that brilliant
community). If you want to help, have suggestions or just want to
say hi, please feel free to ping me on the `trio gitter channel`_!

.. _trio gitter channel: https://gitter.im/python-trio/general


.. contents::


Philosophy
----------
Our tenets non-comprehensively include:

- strict adherence to the `concept-in-progress`_ of *structured concurrency*
- no spawning of processes *willy-nilly*; causality_ is paramount!
- (remote) errors `always propagate`_ back to the parent supervisor
- verbatim support for ``trio``'s cancellation_ system
- `shared nothing architecture`_
- no use of *proxy* objects or shared references between processes
- an immersive debugging experience
- anti-fragility through `chaos engineering`_

``tractor`` is an actor-model-*like* system in the sense that it adheres
to the `3 axioms`_ but does not (yet) fulfil all "unrequirements_" in
practise. It is an experiment in applying `structured concurrency`_
constraints on a parallel processing system where multiple Python
processes exist over many hosts but no process can outlive its parent.
In `erlang` parlance, it is an architecture where every process has
a mandatory supervisor enforced by the type system. The API design is
almost exclusively inspired by trio_'s concepts and primitives (though
we often lag a little). As a distributed computing system `tractor`
attempts to place sophistication at the correct layer such that
concurrency primitives are powerful yet simple, making it easy to build
complex systems (you can build a "worker pool" architecture but it's
definitely not required). There is first class support for inter-actor
streaming using `async generators`_ and ongoing work toward a functional
reactive style for IPC.

.. warning:: ``tractor`` is in alpha-alpha and is expected to change rapidly!
    Expect nothing to be set in stone. Your ideas about where it should go
    are greatly appreciated!

.. _concept-in-progress: https://trio.discourse.group/t/structured-concurrency-kickoff/55
.. _3 axioms: https://en.wikipedia.org/wiki/Actor_model#Fundamental_concepts
.. _unrequirements: https://en.wikipedia.org/wiki/Actor_model#Direct_communication_and_asynchrony
.. _async generators: https://www.python.org/dev/peps/pep-0525/
.. _always propagate: https://trio.readthedocs.io/en/latest/design.html#exceptions-always-propagate
.. _causality: https://vorpus.org/blog/some-thoughts-on-asynchronous-api-design-in-a-post-asyncawait-world/#c-c-c-c-causality-breaker
.. _shared nothing architecture: https://en.wikipedia.org/wiki/Shared-nothing_architecture
.. _cancellation: https://trio.readthedocs.io/en/latest/reference-core.html#cancellation-and-timeouts
.. _channels: https://en.wikipedia.org/wiki/Channel_(programming)
.. _chaos engineering: http://principlesofchaos.org/


Examples
--------
Note, if you are on Windows please be sure to see the :ref:`gotchas
<windowsgotchas>` section before trying these.


A trynamic first scene
**********************
Let's direct a couple *actors* and have them run their lines for
the hip new film we're shooting:

.. literalinclude:: ../examples/a_trynamic_first_scene.py

We spawn two *actors*, *donny* and *gretchen*.
Each actor starts up and executes their *main task* defined by an
async function, ``say_hello()``.  The function instructs each actor
to find their partner and say hello by calling their partner's
``hi()`` function using something called a *portal*. Each actor
receives a response and relays that back to the parent actor (in
this case our "director" executing ``main()``).


Actor spawning and causality
****************************
``tractor`` tries to take ``trio``'s concept of causal task lifetimes
to multi-process land. Accordingly, ``tractor``'s *actor nursery* behaves
similar to ``trio``'s nursery_. That is, ``tractor.open_nursery()``
opens an ``ActorNursery`` which **must** wait on spawned *actors* to complete
(or error) in the same causal_ way ``trio`` waits on spawned subtasks.
This includes errors from any one actor causing all other actors
spawned by the same nursery to be cancelled_.

To spawn an actor and run a function in it, open a *nursery block*
and use the ``run_in_actor()`` method:

.. literalinclude:: ../examples/actor_spawning_and_causality.py

What's going on?

- an initial *actor* is started with ``trio.run()`` and told to execute
  its main task_: ``main()``

- inside ``main()`` an actor is *spawned* using an ``ActorNusery`` and is told
  to run a single function: ``cellar_door()``

- a ``portal`` instance (we'll get to what it is shortly)
  returned from ``nursery.run_in_actor()`` is used to communicate with
  the newly spawned *sub-actor*

- the second actor, *some_linguist*, in a new *process* running a new ``trio`` task_
  then executes ``cellar_door()`` and returns its result over a *channel* back
  to the parent actor

- the parent actor retrieves the subactor's *final result* using ``portal.result()``
  much like you'd expect from a future_.

This ``run_in_actor()`` API should look very familiar to users of
``asyncio``'s `run_in_executor()`_ which uses a ``concurrent.futures`` Executor_.

Since you might also want to spawn long running *worker* or *daemon*
actors, each actor's *lifetime* can be determined based on the spawn
method:

- if the actor is spawned using ``run_in_actor()`` it terminates when
  its *main* task completes (i.e. when the (async) function submitted
  to it *returns*). The ``with tractor.open_nursery()`` exits only once
  all actors' main function/task complete (just like the nursery_ in ``trio``)

- actors can be spawned to *live forever* using the ``start_actor()``
  method and act like an RPC daemon that runs indefinitely (the
  ``with tractor.open_nursery()`` won't exit) until cancelled_

Here is a similar example using the latter method:

.. literalinclude:: ../examples/actor_spawning_and_causality_with_daemon.py

The ``enable_modules`` `kwarg` above is a list of module path
strings that will be loaded and made accessible for execution in the
remote actor through a call to ``Portal.run()``. For now this is
a simple mechanism to restrict the functionality of the remote
(and possibly daemonized) actor and uses Python's module system to
limit the allowed remote function namespace(s).

``tractor`` is opinionated about the underlying threading model used for
each *actor*. Since Python has a GIL and an actor model by definition
shares no state between actors, it fits naturally to use a multiprocessing_
``Process``. This allows ``tractor`` programs to leverage not only multi-core
hardware but also distribute over many hardware hosts (each *actor* can talk
to all others with ease over standard network protocols).

.. _task: https://trio.readthedocs.io/en/latest/reference-core.html#tasks-let-you-do-multiple-things-at-once
.. _nursery: https://trio.readthedocs.io/en/latest/reference-core.html#nurseries-and-spawning
.. _causal: https://vorpus.org/blog/some-thoughts-on-asynchronous-api-design-in-a-post-asyncawait-world/#causality
.. _cancelled: https://trio.readthedocs.io/en/latest/reference-core.html#child-tasks-and-cancellation
.. _run_in_executor(): https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
.. _Executor: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor


Cancellation
************
``tractor`` supports ``trio``'s cancellation_ system verbatim.
Cancelling a nursery block cancels all actors spawned by it.
Eventually ``tractor`` plans to support different `supervision strategies`_ like ``erlang``.

.. _supervision strategies: http://erlang.org/doc/man/supervisor.html#sup_flags


Remote error propagation
************************
Any task invoked in a remote actor should ship any error(s) back to the calling
actor where it is raised and expected to be dealt with. This way remote actors
are never cancelled unless explicitly asked or there's a bug in ``tractor`` itself.

.. literalinclude:: ../examples/remote_error_propagation.py


You'll notice the nursery cancellation conducts a *one-cancels-all*
supervisory strategy `exactly like trio`_. The plan is to add more
`erlang strategies`_ in the near future by allowing nurseries to accept
a ``Supervisor`` type.

.. _exactly like trio: https://trio.readthedocs.io/en/latest/reference-core.html#cancellation-semantics
.. _erlang strategies: http://learnyousomeerlang.com/supervisors


IPC using *portals*
*******************
``tractor`` introduces the concept of a *portal* which is an API
borrowed_ from ``trio``. A portal may seem similar to the idea of
a RPC future_ except a *portal* allows invoking remote *async* functions and
generators and intermittently blocking to receive responses. This allows
for fully async-native IPC between actors.

When you invoke another actor's routines using a *portal* it looks as though
it was called locally in the current actor. So when you see a call to
``await portal.run()`` what you get back is what you'd expect
to if you'd called the function directly in-process. This approach avoids
the need to add any special RPC *proxy* objects to the library by instead just
relying on the built-in (async) function calling semantics and protocols of Python.

Depending on the function type ``Portal.run()`` tries to
correctly interface exactly like a local version of the remote
built-in Python *function type*. Currently async functions, generators,
and regular functions are supported. Inspiration for this API comes
`remote function execution`_ but without the client code being
concerned about the underlying channels_ system or shipping code
over the network.

This *portal* approach turns out to be paricularly exciting with the
introduction of `asynchronous generators`_ in Python 3.6! It means that
actors can compose nicely in a data streaming pipeline.

.. _exactly like trio: https://trio.readthedocs.io/en/latest/reference-core.html#cancellation-semantics

Streaming
*********
By now you've figured out that ``tractor`` lets you spawn process based
*actors* that can invoke cross-process (async) functions and all with
structured concurrency built in. But the **real cool stuff** is the
native support for cross-process *streaming*.


Asynchronous generators
+++++++++++++++++++++++
The default streaming function is simply an async generator definition.
Every value *yielded* from the generator is delivered to the calling
portal exactly like if you had invoked the function in-process meaning
you can ``async for`` to receive each value on the calling side.

As an example here's a parent actor that streams for 1 second from a
spawned subactor:

.. literalinclude:: ../examples/asynchronous_generators.py

By default async generator functions are treated as inter-actor
*streams* when invoked via a portal (how else could you really interface
with them anyway) so no special syntax to denote the streaming *service*
is necessary.


Channels and Contexts
+++++++++++++++++++++
If you aren't fond of having to write an async generator to stream data
between actors (or need something more flexible) you can instead use
a ``Context``. A context wraps an actor-local spawned task and
a ``Channel`` so that tasks executing across multiple processes can
stream data to one another using a low level, request oriented API.

A ``Channel`` wraps an underlying *transport* and *interchange* format
to enable *inter-actor-communication*. In its present state ``tractor``
uses TCP and msgpack_.

As an example if you wanted to create a streaming server without writing
an async generator that *yields* values you instead define a decorated
async function:

.. code:: python

   @tractor.stream
   async def streamer(ctx: tractor.Context, rate: int = 2) -> None:
      """A simple web response streaming server.
      """
      while True:
         val = await web_request('http://data.feed.com')

         # this is the same as ``yield`` in the async gen case
         await ctx.send_yield(val)

         await trio.sleep(1 / rate)


You must decorate the function with ``@tractor.stream`` and declare
a ``ctx`` argument as the first in your function signature and then
``tractor`` will treat the async function like an async generator - as
a stream from the calling/client side.

This turns out to be handy particularly if you have multiple tasks
pushing responses concurrently:

.. code:: python

   async def streamer(
      ctx: tractor.Context,
      rate: int = 2
   ) -> None:
      """A simple web response streaming server.
      """
      while True:
         val = await web_request(url)

         # this is the same as ``yield`` in the async gen case
         await ctx.send_yield(val)

         await trio.sleep(1 / rate)


   @tractor.stream
   async def stream_multiple_sources(
      ctx: tractor.Context,
      sources: List[str]
   ) -> None:
      async with trio.open_nursery() as n:
         for url in sources:
            n.start_soon(streamer, ctx, url)


The context notion comes from the context_ in nanomsg_.

.. _context: https://nanomsg.github.io/nng/man/tip/nng_ctx.5
.. _msgpack: https://en.wikipedia.org/wiki/MessagePack



A full fledged streaming service
++++++++++++++++++++++++++++++++
Alright, let's get fancy.

Say you wanted to spawn two actors which each pull data feeds from
two different sources (and wanted this work spread across 2 cpus).
You also want to aggregate these feeds, do some processing on them and then
deliver the final result stream to a client (or in this case parent) actor
and print the results to your screen:

.. literalinclude:: ../examples/full_fledged_streaming_service.py

Here there's four actors running in separate processes (using all the
cores on you machine). Two are streaming by *yielding* values from the
``stream_data()`` async generator, one is aggregating values from
those two in ``aggregate()`` (also an async generator) and shipping the
single stream of unique values up the parent actor (the ``'MainProcess'``
as ``multiprocessing`` calls it) which is running ``main()``. 

.. _future: https://en.wikipedia.org/wiki/Futures_and_promises
.. _borrowed:
    https://trio.readthedocs.io/en/latest/reference-core.html#getting-back-into-the-trio-thread-from-another-thread
.. _asynchronous generators: https://www.python.org/dev/peps/pep-0525/
.. _remote function execution: https://codespeak.net/execnet/example/test_info.html#remote-exec-a-function-avoiding-inlined-source-part-i


Actor local (aka *process global*) variables
********************************************
Although ``tractor`` uses a *shared-nothing* architecture between
processes you can of course share state between tasks running *within*
an actor (since a `trio.run()` runtime is single threaded). ``trio``
tasks spawned via multiple RPC calls to an actor can modify
*process-global-state* defined using Python module attributes:

.. code:: python


        # a per process cache
        _actor_cache: Dict[str, bool] = {}


        def ping_endpoints(endpoints: List[str]):
            """Start a polling process which runs completely separate
            from our root actor/process.

            """

            # This runs in a new process so no changes # will propagate
            # back to the parent actor
            while True:

                for ep in endpoints:
                    status = await check_endpoint_is_up(ep)
                    _actor_cache[ep] = status

                await trio.sleep(0.5)


        async def get_alive_endpoints():

            nonlocal _actor_cache

            return {key for key, value in _actor_cache.items() if value}


        async def main():

            async with tractor.open_nursery() as n:

                portal = await n.run_in_actor(ping_endpoints)

                # print the alive endpoints after 3 seconds
                await trio.sleep(3)

                # this is submitted to be run in our "ping_endpoints" actor
                print(await portal.run(get_alive_endpoints))


You can pass any kind of (`msgpack`) serializable data between actors using
function call semantics but building out a state sharing system per-actor
is totally up to you.


Service Discovery
*****************
Though it will be built out much more in the near future, ``tractor``
currently keeps track of actors by ``(name: str, id: str)`` using a
special actor called the *arbiter*. Currently the *arbiter* must exist
on a host (or it will be created if one can't be found) and keeps a
simple ``dict`` of actor names to sockets for discovery by other actors.
Obviously this can be made more sophisticated (help me with it!) but for
now it does the trick.

To find the arbiter from the current actor use the ``get_arbiter()`` function and to
find an actor's socket address by name use the ``find_actor()`` function:

.. literalinclude:: ../examples/service_discovery.py

The ``name`` value you should pass to ``find_actor()`` is the one you passed as the
*first* argument to either ``trio.run()`` or ``ActorNursery.start_actor()``.


Running actors standalone
*************************
You don't have to spawn any actors using ``open_nursery()`` if you just
want to run a single actor that connects to an existing cluster.
All the comms and arbiter registration stuff still works. This can
somtimes turn out being handy when debugging mult-process apps when you
need to hop into a debugger. You just need to pass the existing
*arbiter*'s socket address you'd like to connect to:

.. code:: python

    import trio
    import tractor

    async def main():

        async with tractor.open_root_actor(
            arbiter_addr=('192.168.0.10', 1616)
        ):
            await trio.sleep_forever()

    trio.run(main)


Choosing a process spawning backend
***********************************
``tractor`` is architected to support multiple actor (sub-process)
spawning backends. Specific defaults are chosen based on your system
but you can also explicitly select a backend of choice at startup
via a ``start_method`` kwarg to ``tractor.open_nursery()``.

Currently the options available are:

- ``trio``: a ``trio``-native spawner which is an async wrapper around ``subprocess``
- ``spawn``: one of the stdlib's ``multiprocessing`` `start methods`_
- ``forkserver``: a faster ``multiprocessing`` variant that is Unix only

.. _start methods: https://docs.python.org/3.8/library/multiprocessing.html#contexts-and-start-methods


``trio``
++++++++
The ``trio`` backend offers a lightweight async wrapper around ``subprocess`` from the standard library and takes advantage of the ``trio.`` `open_process`_ API.

.. _open_process: https://trio.readthedocs.io/en/stable/reference-io.html#spawning-subprocesses


``multiprocessing``
+++++++++++++++++++
There is support for the stdlib's ``multiprocessing`` `start methods`_.
Note that on Windows *spawn* it the only supported method and on \*nix
systems *forkserver* is the best method for speed but has the caveat
that it will break easily (hangs due to broken pipes) if spawning actors
using nested nurseries.

In general, the ``multiprocessing`` backend **has not proven reliable**
for handling errors from actors more then 2 nurseries *deep* (see `#89`_).
If you for some reason need this consider sticking with alternative
backends.

.. _#89: https://github.com/goodboy/tractor/issues/89

.. _windowsgotchas:

Windows "gotchas"
^^^^^^^^^^^^^^^^^
On Windows (which requires the use of the stdlib's `multiprocessing`
package) there are some gotchas. Namely, the need for calling
`freeze_support()`_ inside the ``__main__`` context.  Additionally you
may need place you `tractor` program entry point in a seperate
`__main__.py` module in your package in order to avoid an error like the
following ::

    Traceback (most recent call last):
      File "C:\ProgramData\Miniconda3\envs\tractor19030601\lib\site-packages\tractor\_actor.py", line 234, in _get_rpc_func
        return getattr(self._mods[ns], funcname)
    KeyError: '__mp_main__'


To avoid this, the following is the **only code** that should be in your
main python module of the program:

.. code:: python

    # application/__main__.py
    import trio
    import tractor
    import multiprocessing
    from . import tractor_app

    if __name__ == '__main__':
        multiprocessing.freeze_support()
        trio.run(tractor_app.main)

And execute as::

    python -m application


As an example we use the following code to test all documented examples
in the test suite on windows:

.. literalinclude:: ../examples/__main__.py

See `#61`_ and `#79`_ for further details.

.. _freeze_support(): https://docs.python.org/3/library/multiprocessing.html#multiprocessing.freeze_support
.. _#61: https://github.com/goodboy/tractor/pull/61#issuecomment-470053512
.. _#79: https://github.com/goodboy/tractor/pull/79


Enabling logging
****************
Considering how complicated distributed software can become it helps to know
what exactly it's doing (even at the lowest levels). Luckily ``tractor`` has
tons of logging throughout the core. ``tractor`` isn't opinionated on
how you use this information and users are expected to consume log messages in
whichever way is appropriate for the system at hand. That being said, when hacking
on ``tractor`` there is a prettified console formatter which you can enable to
see what the heck is going on. Just put the following somewhere in your code:

.. code:: python

    from tractor.log import get_console_log
    log = get_console_log('trace')


What the future holds
---------------------
Stuff I'd like to see ``tractor`` do real soon:

- TLS_, duh.
- erlang-like supervisors_
- native support for `nanomsg`_ as a channel transport
- native `gossip protocol`_ support for service discovery and arbiter election
- a distributed log ledger for tracking cluster behaviour
- a slick multi-process aware debugger much like in celery_
  but with better `pdb++`_ support
- an extensive `chaos engineering`_ test suite
- support for reactive programming primitives and native support for asyncitertools_ like libs
- introduction of a `capability-based security`_ model

.. _TLS: https://trio.readthedocs.io/en/latest/reference-io.html#ssl-tls-support
.. _supervisors: https://github.com/goodboy/tractor/issues/22
.. _nanomsg: https://nanomsg.github.io/nng/index.html
.. _gossip protocol: https://en.wikipedia.org/wiki/Gossip_protocol
.. _celery: http://docs.celeryproject.org/en/latest/userguide/debugging.html
.. _asyncitertools: https://github.com/vodik/asyncitertools
.. _pdb++: https://github.com/antocuni/pdb
.. _capability-based security: https://en.wikipedia.org/wiki/Capability-based_security

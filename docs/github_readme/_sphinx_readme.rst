tractor
=======
The Python async-native multi-core system *you always wanted*.


|gh_actions|
|docs|

.. _actor model: https://en.wikipedia.org/wiki/Actor_model
.. _trio: https://github.com/python-trio/trio
.. _multi-processing: https://en.wikipedia.org/wiki/Multiprocessing
.. _trionic: https://trio.readthedocs.io/en/latest/design.html#high-level-design-principles
.. _async sandwich: https://trio.readthedocs.io/en/latest/tutorial.html#async-sandwich
.. _structured concurrent: https://trio.discourse.group/t/concise-definition-of-structured-concurrency/228


``tractor`` is a `structured concurrent`_ "`actor model`_" built on trio_ and multi-processing_.

It is an attempt to pair trionic_ `structured concurrency`_ with
distributed Python. You can think of it as a ``trio``
*-across-processes* or simply as an opinionated replacement for the
stdlib's ``multiprocessing`` but built on async programming primitives
from the ground up.

Don't be scared off by this description. ``tractor`` **is just ``trio``**
but with nurseries for process management and cancel-able IPC.
If you understand how to work with ``trio``, ``tractor`` will give you
the parallelism you've been missing.

``tractor``'s nurseries let you spawn ``trio`` *"actors"*: new Python
processes which each run a ``trio`` scheduled task tree (also known as
an `async sandwich`_ - a call to ``trio.run()``). That is, each
"*Actor*" is a new process plus a ``trio`` runtime.

"Actors" communicate by exchanging asynchronous messages_ and avoid
sharing state. The intention of this model is to allow for highly
distributed software that, through the adherence to *structured
concurrency*, results in systems which fail in predictable and
recoverable ways.

The first step to grok ``tractor`` is to get the basics of ``trio`` down.
A great place to start is the `trio docs`_ and this `blog post`_.

.. _messages: https://en.wikipedia.org/wiki/Message_passing
.. _trio docs: https://trio.readthedocs.io/en/latest/
.. _blog post: https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
.. _structured concurrency: https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
.. _3 axioms: https://en.wikipedia.org/wiki/Actor_model#Fundamental_concepts
.. _unrequirements: https://en.wikipedia.org/wiki/Actor_model#Direct_communication_and_asynchrony
.. _async generators: https://www.python.org/dev/peps/pep-0525/


Install
-------
No PyPi release yet!

::

    pip install git+git://github.com/goodboy/tractor.git


Alluring Features
-----------------
- **It's just** ``trio``, but with SC applied to processes (aka "actors")
- Infinitely nesteable process trees
- Built-in API for inter-process streaming
- A (first ever?) "native" multi-core debugger for Python using `pdb++`_
- (Soon to land) ``asyncio`` support allowing for "infected" actors where
  `trio` drives the `asyncio` scheduler via the astounding "`guest mode`_"


Example: self-destruct a process tree
-------------------------------------
.. literalinclude:: ../../examples/parallelism/we_are_processes.py
    :language: python


The example you're probably after...
------------------------------------
It seems the initial query from most new users is "how do I make a worker
pool thing?".

``tractor`` is built to handle any SC process tree you can
imagine; the "worker pool" pattern is a trivial special case:

.. literalinclude:: ../../examples/parallelism/concurrent_actors_primes.py
    :language: python


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


.. |gh_actions| image:: https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fgoodboy%2Ftractor%2Fbadge&style=popout-square
    :target: https://actions-badge.atrox.dev/goodboy/tractor/goto
.. |docs| image:: https://readthedocs.org/projects/tractor/badge/?version=latest
    :target: https://tractor.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

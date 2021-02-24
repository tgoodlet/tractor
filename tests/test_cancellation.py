"""
Cancellation and error propagation
"""
import os
import signal
import platform
import time
from itertools import repeat

import pytest
import trio
import tractor

from conftest import tractor_test, no_windows


async def assert_err(delay=0):
    await trio.sleep(delay)
    assert 0


async def sleep_forever():
    await trio.sleep_forever()


async def do_nuthin():
    # just nick the scheduler
    await trio.sleep(0)


@pytest.mark.parametrize(
    'args_err',
    [
        # expected to be thrown in assert_err
        ({}, AssertionError),
        # argument mismatch raised in _invoke()
        ({'unexpected': 10}, TypeError)
    ],
    ids=['no_args', 'unexpected_args'],
)
def test_remote_error(arb_addr, args_err):
    """Verify an error raised in a subactor that is propagated
    to the parent nursery, contains the underlying boxed builtin
    error type info and causes cancellation and reraising all the
    way up the stack.
    """
    args, errtype = args_err

    async def main():
        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
        ) as nursery:

            portal = await nursery.run_in_actor(
                assert_err, name='errorer', **args
            )

            # get result(s) from main task
            try:
                await portal.result()
            except tractor.RemoteActorError as err:
                assert err.type == errtype
                print("Look Maa that actor failed hard, hehh")
                raise

    with pytest.raises(tractor.RemoteActorError) as excinfo:
        trio.run(main)

    # ensure boxed error is correct
    assert excinfo.value.type == errtype


def test_multierror(arb_addr):
    """Verify we raise a ``trio.MultiError`` out of a nursery where
    more then one actor errors.
    """
    async def main():
        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
        ) as nursery:

            await nursery.run_in_actor(assert_err, name='errorer1')
            portal2 = await nursery.run_in_actor(assert_err, name='errorer2')

            # get result(s) from main task
            try:
                await portal2.result()
            except tractor.RemoteActorError as err:
                assert err.type == AssertionError
                print("Look Maa that first actor failed hard, hehh")
                raise

        # here we should get a `trio.MultiError` containing exceptions
        # from both subactors

    with pytest.raises(trio.MultiError):
        trio.run(main)


@pytest.mark.parametrize('delay', (0, 0.5))
@pytest.mark.parametrize(
    'num_subactors', range(25, 26),
)
def test_multierror_fast_nursery(arb_addr, start_method, num_subactors, delay):
    """Verify we raise a ``trio.MultiError`` out of a nursery where
    more then one actor errors and also with a delay before failure
    to test failure during an ongoing spawning.
    """
    async def main():
        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
        ) as nursery:

            for i in range(num_subactors):
                await nursery.run_in_actor(
                    assert_err,
                    name=f'errorer{i}',
                    delay=delay
                )

    with pytest.raises(trio.MultiError) as exc_info:
        trio.run(main)

    assert exc_info.type == tractor.MultiError
    err = exc_info.value
    assert len(err.exceptions) == num_subactors
    for exc in err.exceptions:
        assert isinstance(exc, tractor.RemoteActorError)
        assert exc.type == AssertionError


async def do_nothing():
    pass


@pytest.mark.parametrize('mechanism', ['nursery_cancel', KeyboardInterrupt])
def test_cancel_single_subactor(arb_addr, mechanism):
    """Ensure a ``ActorNursery.start_actor()`` spawned subactor
    cancels when the nursery is cancelled.
    """
    async def spawn_actor():
        """Spawn an actor that blocks indefinitely.
        """
        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
        ) as nursery:

            portal = await nursery.start_actor(
                'nothin', enable_modules=[__name__],
            )
            assert (await portal.run(do_nothing)) is None

            if mechanism == 'nursery_cancel':
                # would hang otherwise
                await nursery.cancel()
            else:
                raise mechanism

    if mechanism == 'nursery_cancel':
        trio.run(spawn_actor)
    else:
        with pytest.raises(mechanism):
            trio.run(spawn_actor)


async def stream_forever():
    for i in repeat("I can see these little future bubble things"):
        # each yielded value is sent over the ``Channel`` to the
        # parent actor
        yield i
        await trio.sleep(0.01)


@tractor_test
async def test_cancel_infinite_streamer(start_method):

    # stream for at most 1 seconds
    with trio.move_on_after(1) as cancel_scope:
        async with tractor.open_nursery() as n:
            portal = await n.start_actor(
                'donny',
                enable_modules=[__name__],
            )

            # this async for loop streams values from the above
            # async generator running in a separate process
            async with portal.open_stream_from(stream_forever) as stream:
                async for letter in stream:
                    print(letter)

    # we support trio's cancellation system
    assert cancel_scope.cancelled_caught
    assert n.cancelled


@pytest.mark.parametrize(
    'num_actors_and_errs',
    [
        # daemon actors sit idle while single task actors error out
        (1, tractor.RemoteActorError, AssertionError, (assert_err, {}), None),
        (2, tractor.MultiError, AssertionError, (assert_err, {}), None),
        (3, tractor.MultiError, AssertionError, (assert_err, {}), None),

        # 1 daemon actor errors out while single task actors sleep forever
        (3, tractor.RemoteActorError, AssertionError, (sleep_forever, {}),
         (assert_err, {}, True)),
        # daemon actors error out after brief delay while single task
        # actors complete quickly
        (3, tractor.RemoteActorError, AssertionError,
         (do_nuthin, {}), (assert_err, {'delay': 1}, True)),
        # daemon complete quickly delay while single task
        # actors error after brief delay
        (3, tractor.MultiError, AssertionError,
         (assert_err, {'delay': 1}), (do_nuthin, {}, False)),
    ],
    ids=[
        '1_run_in_actor_fails',
        '2_run_in_actors_fail',
        '3_run_in_actors_fail',
        '1_daemon_actors_fail',
        '1_daemon_actors_fail_all_run_in_actors_dun_quick',
        'no_daemon_actors_fail_all_run_in_actors_sleep_then_fail',
    ],
)
@tractor_test
async def test_some_cancels_all(num_actors_and_errs, start_method, loglevel):
    """Verify a subset of failed subactors causes all others in
    the nursery to be cancelled just like the strategy in trio.

    This is the first and only supervisory strategy at the moment.
    """
    num_actors, first_err, err_type, ria_func, da_func = num_actors_and_errs
    try:
        async with tractor.open_nursery() as n:

            # spawn the same number of deamon actors which should be cancelled
            dactor_portals = []
            for i in range(num_actors):
                dactor_portals.append(await n.start_actor(
                    f'deamon_{i}',
                    enable_modules=[__name__],
                ))

            func, kwargs = ria_func
            riactor_portals = []
            for i in range(num_actors):
                # start actor(s) that will fail immediately
                riactor_portals.append(
                    await n.run_in_actor(
                        func,
                        name=f'actor_{i}',
                        **kwargs
                    )
                )

            if da_func:
                func, kwargs, expect_error = da_func
                for portal in dactor_portals:
                    # if this function fails then we should error here
                    # and the nursery should teardown all other actors
                    try:
                        await portal.run(func, **kwargs)

                    except tractor.RemoteActorError as err:
                        assert err.type == err_type
                        # we only expect this first error to propogate
                        # (all other daemons are cancelled before they
                        # can be scheduled)
                        num_actors = 1
                        # reraise so nursery teardown is triggered
                        raise
                    else:
                        if expect_error:
                            pytest.fail(
                                "Deamon call should fail at checkpoint?")

        # should error here with a ``RemoteActorError`` or ``MultiError``

    except first_err as err:
        if isinstance(err, tractor.MultiError):
            assert len(err.exceptions) == num_actors
            for exc in err.exceptions:
                if isinstance(exc, tractor.RemoteActorError):
                    assert exc.type == err_type
                else:
                    assert isinstance(exc, trio.Cancelled)
        elif isinstance(err, tractor.RemoteActorError):
            assert err.type == err_type

        assert n.cancelled is True
        assert not n._children
    else:
        pytest.fail("Should have gotten a remote assertion error?")


async def spawn_and_error(breadth, depth) -> None:
    name = tractor.current_actor().name
    async with tractor.open_nursery() as nursery:
        for i in range(breadth):

            if depth > 0:

                args = (
                    spawn_and_error,
                )
                kwargs = {
                    'name': f'spawner_{i}_depth_{depth}',
                    'breadth': breadth,
                    'depth': depth - 1,
                }
            else:
                args = (
                    assert_err,
                )
                kwargs = {
                    'name': f'{name}_errorer_{i}',
                }
            await nursery.run_in_actor(*args, **kwargs)


@tractor_test
async def test_nested_multierrors(loglevel, start_method):
    """Test that failed actor sets are wrapped in `trio.MultiError`s.
    This test goes only 2 nurseries deep but we should eventually have tests
    for arbitrary n-depth actor trees.
    """
    if start_method == 'trio':
        depth = 3
        subactor_breadth = 2
    else:
        # XXX: multiprocessing can't seem to handle any more then 2 depth
        # process trees for whatever reason.
        # Any more process levels then this and we see bugs that cause
        # hangs and broken pipes all over the place...
        if start_method == 'forkserver':
            pytest.skip("Forksever sux hard at nested spawning...")
        depth = 1  # means an additional actor tree of spawning (2 levels deep)
        subactor_breadth = 2

    with trio.fail_after(120):
        try:
            async with tractor.open_nursery() as nursery:
                for i in range(subactor_breadth):
                    await nursery.run_in_actor(
                        spawn_and_error,
                        name=f'spawner_{i}',
                        breadth=subactor_breadth,
                        depth=depth,
                    )
        except trio.MultiError as err:
            assert len(err.exceptions) == subactor_breadth
            for subexc in err.exceptions:

                # verify first level actor errors are wrapped as remote
                if platform.system() == 'Windows':

                    # windows is often too slow and cancellation seems
                    # to happen before an actor is spawned
                    if isinstance(subexc, trio.Cancelled):
                        continue
                    else:
                        # on windows it seems we can't exactly be sure wtf
                        # will happen..
                        assert subexc.type in (
                            tractor.RemoteActorError,
                            trio.Cancelled,
                            trio.MultiError
                        )
                else:
                    assert isinstance(subexc, tractor.RemoteActorError)

                if depth > 0 and subactor_breadth > 1:
                    # XXX not sure what's up with this..
                    # on windows sometimes spawning is just too slow and
                    # we get back the (sent) cancel signal instead
                    if platform.system() == 'Windows':
                        assert (subexc.type is trio.MultiError) or (
                            subexc.type is tractor.RemoteActorError)
                    else:
                        assert subexc.type is trio.MultiError
                else:
                    assert (subexc.type is tractor.RemoteActorError) or (
                        subexc.type is trio.Cancelled)


@no_windows
def test_cancel_via_SIGINT(
    loglevel,
    start_method,
    spawn_backend,
):
    """Ensure that a control-C (SIGINT) signal cancels both the parent and
    child processes in trionic fashion
    """
    pid = os.getpid()

    async def main():
        with trio.fail_after(2):
            async with tractor.open_nursery() as tn:
                await tn.start_actor('sucka')
                if spawn_backend == 'mp':
                    time.sleep(0.1)
                os.kill(pid, signal.SIGINT)
                await trio.sleep_forever()

    with pytest.raises(KeyboardInterrupt):
        trio.run(main)


@no_windows
def test_cancel_via_SIGINT_other_task(
    loglevel,
    start_method,
    spawn_backend,
):
    """Ensure that a control-C (SIGINT) signal cancels both the parent
    and child processes in trionic fashion even a subprocess is started
    from a seperate ``trio`` child  task.
    """
    pid = os.getpid()

    async def spawn_and_sleep_forever(task_status=trio.TASK_STATUS_IGNORED):
        async with tractor.open_nursery() as tn:
            for i in range(3):
                await tn.run_in_actor(
                    sleep_forever,
                    name='namesucka',
                )
            task_status.started()
            await trio.sleep_forever()

    async def main():
        # should never timeout since SIGINT should cancel the current program
        with trio.fail_after(2):
            async with trio.open_nursery() as n:
                await n.start(spawn_and_sleep_forever)
                if spawn_backend == 'mp':
                    time.sleep(0.1)
                os.kill(pid, signal.SIGINT)

    with pytest.raises(KeyboardInterrupt):
        trio.run(main)

async def spin_for(period=3):
    "Sync sleep."
    time.sleep(period)


async def spawn():
    async with tractor.open_nursery() as tn:
        await tn.run_in_actor(
            spin_for,
            name='sleeper',
        )


@no_windows
def test_cancel_while_childs_child_in_sync_sleep(
    loglevel,
    start_method,
    spawn_backend,
):
    """Verify that a child cancelled while executing sync code is torn
    down even when that cancellation is triggered by the parent
    2 nurseries "up".
    """
    if start_method == 'forkserver':
        pytest.skip("Forksever sux hard at resuming from sync sleep...")

    async def main():
        with trio.fail_after(2):
            async with tractor.open_nursery() as tn:
                await tn.run_in_actor(
                    spawn,
                    name='spawn',
                )
                await trio.sleep(1)
                assert 0

    with pytest.raises(AssertionError):
        trio.run(main)

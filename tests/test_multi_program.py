"""
Multiple python programs invoking the runtime.
"""
import platform
import time

import pytest
import trio
import tractor
from conftest import (
    tractor_test,
    sig_prog,
    _INT_SIGNAL,
    _INT_RETURN_CODE,
)


def test_abort_on_sigint(daemon):
    assert daemon.returncode is None
    time.sleep(0.1)
    sig_prog(daemon, _INT_SIGNAL)
    assert daemon.returncode == _INT_RETURN_CODE

    # XXX: oddly, couldn't get capfd.readouterr() to work here?
    if platform.system() != 'Windows':
        # don't check stderr on windows as its empty when sending CTRL_C_EVENT
        assert "KeyboardInterrupt" in str(daemon.stderr.read())


@tractor_test
async def test_cancel_remote_arbiter(daemon, arb_addr):
    assert not tractor.current_actor().is_arbiter
    async with tractor.get_arbiter(*arb_addr) as portal:
        await portal.cancel_actor()

    time.sleep(0.1)
    # the arbiter channel server is cancelled but not its main task
    assert daemon.returncode is None

    # no arbiter socket should exist
    with pytest.raises(OSError):
        async with tractor.get_arbiter(*arb_addr) as portal:
            pass


def test_register_duplicate_name(daemon, arb_addr):

    async def main():

        async with tractor.open_nursery(
            arbiter_addr=arb_addr,
        ) as n:

            assert not tractor.current_actor().is_arbiter

            p1 = await n.start_actor('doggy')
            p2 = await n.start_actor('doggy')

            async with tractor.wait_for_actor('doggy') as portal:
                assert portal.channel.uid in (p2.channel.uid, p1.channel.uid)

            await n.cancel()

    # run it manually since we want to start **after**
    # the other "daemon" program
    trio.run(main)

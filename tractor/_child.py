"""This is the "bootloader" for actors started using the native trio backend.
"""
import sys
import trio
import argparse

from ast import literal_eval

from ._actor import Actor
from ._entry import _trio_main


def parse_uid(arg):
    name, uuid = literal_eval(arg)  # ensure 2 elements
    return str(name), str(uuid)  # ensures str encoding

def parse_ipaddr(arg):
    host, port = literal_eval(arg)
    return (str(host), int(port))


from ._entry import _trio_main

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--uid", type=parse_uid)
    parser.add_argument("--loglevel", type=str)
    parser.add_argument("--parent_addr", type=parse_ipaddr)
    parser.add_argument("--asyncio", action='store_true')
    args = parser.parse_args()

    subactor = Actor(
        args.uid[0],
        uid=args.uid[1],
        loglevel=args.loglevel,
        spawn_method="trio"
    )

    _trio_main(
        subactor,
        parent_addr=args.parent_addr,
        infect_asyncio=args.asyncio,
    )

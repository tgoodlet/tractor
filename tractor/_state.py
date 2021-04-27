"""
Per process state
"""
from typing import Optional, Dict, Any
from collections.abc import Mapping
import multiprocessing as mp

import trio

_current_actor: Optional['Actor'] = None  # type: ignore # noqa
_runtime_vars: Dict[str, Any] = {
    '_debug_mode': False,
    '_is_root': False,
    '_root_mailbox': (None, None)
}


def current_actor(
    err_on_no_runtime: bool = True,
    # due_to_logging: bool = False,
) -> 'Actor':  # type: ignore # noqa
    """Get the process-local actor instance.
    """
    if _current_actor is None and err_on_no_runtime:
        # if not due_to_logging:
        #     print('uhhhh')
        #     breakpoint()
        raise RuntimeError("No local actor has been initialized yet")

    return _current_actor


_conc_name_getters = {
    'task': trio.lowlevel.current_task,
    'actor': current_actor
}


class ActorContextInfo(Mapping):
    "Dyanmic lookup for local actor and task names"
    _context_keys = ('task', 'actor')

    def __len__(self):
        return len(self._context_keys)

    def __iter__(self):
        return iter(self._context_keys)

    def __getitem__(self, key: str) -> str:
        try:
            # if key == 'actor':
            #     return _conc_name_getters[key](due_to_logging=True).name  # type: ignore
            # else:
            return _conc_name_getters[key]().name  # type: ignore
        except RuntimeError:
            # no local actor/task context initialized yet
            return f'no {key} context'


def is_main_process() -> bool:
    """Bool determining if this actor is running in the top-most process.
    """
    return mp.current_process().name == 'MainProcess'


def debug_mode() -> bool:
    """Bool determining if "debug mode" is on which enables
    remote subactor pdb entry on crashes.
    """
    return bool(_runtime_vars['_debug_mode'])


def is_root_process() -> bool:
    return _runtime_vars['_is_root']

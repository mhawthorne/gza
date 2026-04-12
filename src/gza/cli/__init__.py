"""Command-line interface for Gza.

This package re-exports all public symbols so that ``from gza.cli import X``
continues to work while the implementation is split across sub-modules.

**Important for tests**: Always patch via ``patch("gza.cli.X", ...)``, never
via ``patch("gza.cli.<submodule>.X", ...)``.  The ``_CliModule.__setattr__``
hook automatically propagates patches set on ``gza.cli`` to every sub-module
that has that attribute.  Patching a sub-module directly will bypass this
mechanism and leave other sub-modules using the real implementation.
"""

import sys as _sys
import types as _types

from . import (
    _common as _common_mod,  # noqa: F401
    config_cmds as _config_cmds_mod,  # noqa: F401
    execution as _execution_mod,  # noqa: F401
    git_ops as _git_ops_mod,  # noqa: F401
    log as _log_mod,  # noqa: F401
    query as _query_mod,  # noqa: F401
    watch as _watch_mod,  # noqa: F401
)
from .main import main  # noqa: F401

# Sub-modules list for attribute propagation
_SUBMODULES = (_common_mod, _log_mod, _config_cmds_mod, _query_mod, _git_ops_mod, _execution_mod, _watch_mod)

# Build mapping: attribute name -> list of sub-modules that have it.
# When a test does patch("gza.cli.X", mock), we propagate the mock to every
# sub-module that has attribute X, so the patched version is used at call time.
_ATTR_OWNERS: dict[str, list[_types.ModuleType]] = {}
for _mod in _SUBMODULES:
    for _name in dir(_mod):
        if not _name.startswith("__"):
            _ATTR_OWNERS.setdefault(_name, []).append(_mod)

# Copy all sub-module attributes onto this package module so that
# ``from gza.cli import X`` and ``gza.cli.X`` work.
_self = _sys.modules[__name__]
for _mod in _SUBMODULES:
    for _name in dir(_mod):
        if not _name.startswith("__"):
            setattr(_self, _name, getattr(_mod, _name))


class _CliModule(_types.ModuleType):
    """Custom module type that propagates attribute patches to sub-modules.

    When tests do ``patch("gza.cli._spawn_background_worker", mock)``, this
    ensures the mock also replaces the binding in every sub-module that has
    that attribute, so the patched version is used at call time.
    """

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        owners = _ATTR_OWNERS.get(name)
        if owners:
            for mod in owners:
                _types.ModuleType.__setattr__(mod, name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        owners = _ATTR_OWNERS.get(name)
        if owners:
            for mod in owners:
                try:
                    _types.ModuleType.__delattr__(mod, name)
                except AttributeError:
                    pass


# Replace this module's class so __setattr__ hooks work
_self.__class__ = _CliModule

del _self, _mod, _name

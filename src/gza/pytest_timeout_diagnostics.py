"""Shared SIGTERM diagnostics for pytest-based verification harnesses."""

from .test_harness import positive_int_env, register_sigterm_faulthandler

__all__ = ["positive_int_env", "register_sigterm_faulthandler"]

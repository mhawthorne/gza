"""Shared SIGTERM diagnostics for pytest-based verification harnesses."""

from .test_harness import register_sigterm_faulthandler as register_sigterm_faulthandler

__all__ = ["register_sigterm_faulthandler"]

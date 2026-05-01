"""Fulcrum package."""

from __future__ import annotations

from importlib import import_module

__all__ = ["create_fulcrum_app"]


def __getattr__(name: str):
    if name == "create_fulcrum_app":
        return import_module("app.fulcrum.app").create_fulcrum_app
    try:
        return import_module(f"app.fulcrum.{name}")
    except ModuleNotFoundError as exc:  # pragma: no cover - normal attribute error path
        raise AttributeError(f"module 'app.fulcrum' has no attribute {name!r}") from exc

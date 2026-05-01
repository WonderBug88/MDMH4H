"""Fulcrum-local wrappers for scheduled and background jobs."""

from __future__ import annotations


def run_gsc_refresh():
    from app.fulcrum.gsc_refresh import gsc_weekly_update_main

    return gsc_weekly_update_main()

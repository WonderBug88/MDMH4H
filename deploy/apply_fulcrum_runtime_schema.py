"""Apply the Route Authority runtime schema once and exit."""

from __future__ import annotations

from app.fulcrum.services import apply_runtime_schema


def main() -> int:
    apply_runtime_schema()
    print("Fulcrum runtime schema is applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


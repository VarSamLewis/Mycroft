#!/usr/bin/env bash
set -euo pipefail

echo "==> Running Python tests"
uv run pytest tests/backend/unit_tests.py

echo "==> Running Python lint (ruff)"
uv run ruff check .

echo "==> Checking Python formatting"
uv run ruff format --check .

echo "==> Running Python type check (mypy)"
uv run mypy --no-site-packages src/mycroft

echo "==> Running frontend lint"
npm run lint --prefix src/frontend

echo "==> Running frontend type check"
npm run typecheck --prefix src/frontend

echo "==> All checks passed"

#!/usr/bin/env bash
set -euo pipefail
python -m py_compile app/engine_datamaster.py
python -m app.smoke_test

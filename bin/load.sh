#!/usr/bin/env bash

set -euo pipefail

cat "$1" | duckdb -f ./sql/load_data.sql "$2"

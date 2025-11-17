#!/usr/bin/env bash

set -euo pipefail

RAW_DIR="data/raw/worldbank"

mkdir -p "$RAW_DIR"

fetch_zip() {
    URL="$1"
    tmp="$(mktemp)"
    trap 'rm -f "$tmp"' EXIT

    if ! curl -fsSL "$URL" -o "$tmp"; then
        echo "Download of $URL failed" >&2
        return 1
    fi

    if ! unzip -o "$tmp" -d "$RAW_DIR"; then
        echo "Extraction of $URL failed" >&2
        return 1
    fi
    rm -f "$tmp"
    echo "Fetched and extracted $URL to $RAW_DIR"
    return 0
}

# GDP (current US$)
fetch_zip "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=csv"
# Gini index
fetch_zip "https://api.worldbank.org/v2/en/indicator/SI.POV.GINI?downloadformat=csv"
# Foreign direct investment, net inflows (BoP, current US$)
fetch_zip "https://api.worldbank.org/v2/en/indicator/BX.KLT.DINV.CD.WD?downloadformat=csv"

sed -i 's/\r$//' $RAW_DIR/*.csv

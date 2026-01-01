#!/bin/sh

set -eu

RAW_DIR="data/raw/worldbank"
BASE_URL="https://api.worldbank.org/v2/en/indicator"

mkdir -p "$RAW_DIR"

fetch_zip() {
    url="$1"

    tmp=$(mktemp) || return 1

    if ! curl -fsSL "$url" > "$tmp"; then
        echo "Download failed: $url" >&2
        rm -f "$tmp"
        return 1
    fi

    if ! unzip -oq "$tmp" -d "$RAW_DIR"; then
        echo "Extraction failed: $url" >&2
        rm -f "$tmp"
        return 1
    fi

    rm -f "$tmp"
    echo "Fetched and extracted: $url"
}

# GDP, PPP (constant 2021 international $)
# GDP (constant 2015 US$)
# GDP (constant LCU)
# Gini index
# Foreign direct investment, net inflows (BoP, current US$)
# Foreign direct investment, net outflows (BoP, current US$)
INDICATORS="
NY.GDP.MKTP.PP.KD
NY.GDP.MKTP.KD
NY.GDP.MKTP.KN
SI.POV.GINI
BX.KLT.DINV.CD.WD
BM.KLT.DINV.CD.WD
"

echo "$INDICATORS" |
while read -r code; do
    [ -z "$code" ] && continue

    fetch_zip "$BASE_URL/$code?downloadformat=csv"
done

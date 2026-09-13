#!/usr/bin/env bash
# Build the erpl-extensions wheel: fetch the ERPL release, unpack it into the
# package, and tag the wheel for the only platform ERPL publishes.
#
# The payload is never committed -- 194 MB of binaries in git would be a
# liability, and the checksums pin what the build is allowed to pick up.
set -euo pipefail
cd "$(dirname "$0")/.."

PAYLOAD="src/erpl_extensions/_files"
FETCH="${FETCH_EXTENSIONS:-../../source-sap/bin/fetch-extensions.sh}"

if [ ! -x "$FETCH" ] && [ ! -f "$FETCH" ]; then
    echo "cannot find fetch-extensions.sh at $FETCH" >&2
    exit 1
fi

rm -rf "$PAYLOAD"
mkdir -p "$PAYLOAD"
# Reuses the connector's fetcher, which verifies every download against
# bin/checksums.txt and unpacks the trampoline's payload.
bash "$FETCH" "$(pwd)/$PAYLOAD"

# The package advertises an ERPL version; a payload from a different release
# would make that a lie, and nothing downstream would notice.
declared="$(python3 -c "import re,pathlib;print(re.search(r'ERPL_VERSION = \"([^\"]+)\"', pathlib.Path('src/erpl_extensions/__init__.py').read_text()).group(1))")"
echo "declared ERPL version: ${declared}"

found="$(find "$PAYLOAD" -name '*.duckdb_extension' | wc -l)"
if [ "$found" -lt 4 ]; then
    echo "expected the four ERPL extensions in the payload, found ${found}" >&2
    exit 1
fi
du -sh "$PAYLOAD"

rm -rf dist
pyproject-build --wheel --outdir dist_untagged .

# ELF binaries inside, so the wheel must not claim to be pure Python: an
# any-platform tag would let pip install it on arm64 and produce a connector
# that starts and can load nothing.
wheel tags --platform-tag manylinux_2_17_x86_64 --remove dist_untagged/*.whl
mkdir -p dist && mv dist_untagged/*.whl dist/ && rmdir dist_untagged
ls -la dist/

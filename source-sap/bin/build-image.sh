#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

TAG="$(python3 -c "import re,pathlib;print(re.search(r'dockerImageTag: (\S+)', pathlib.Path('metadata.yaml').read_text()).group(1))")"
REPO="$(python3 -c "import re,pathlib;print(re.search(r'dockerRepository: (\S+)', pathlib.Path('metadata.yaml').read_text()).group(1))")"

docker build --platform linux/amd64 -t "${REPO}:${TAG}" -t "${REPO}:dev" .
echo "built ${REPO}:${TAG}"

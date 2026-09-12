#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

TAG="$(python3 -c "import tomllib,pathlib;print(tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['version'])")"
REPO="$(python3 -c "import re,pathlib;print(re.search(r'dockerRepository: (\S+)', pathlib.Path('metadata.yaml').read_text()).group(1))")"

docker build --platform linux/amd64 -t "${REPO}:${TAG}" -t "${REPO}:dev" .
echo "built ${REPO}:${TAG}"

#!/bin/bash

docker run --rm --init -i \
    -w /data/208afea3-80f7-4648-bc53-ccb08f000a26/0 \
    --log-driver none --name source_sapreadtable-spec-208afea3-80f7-4648-bc53-ccb08f000a26-0-ygghe \
    --network host \
    -v airbyte_workspace:/data \
    -v oss_local_root:/local \
    -e DEPLOYMENT_MODE=OSS \
    -e WORKER_CONNECTOR_IMAGE=public.ecr.aws/l0a4a0e7/source_sapreadtable:latest \
    -e AUTO_DETECT_SCHEMA=true \
    -e LAUNCHDARKLY_KEY= \
    -e SOCAT_KUBE_CPU_REQUEST=0.1 \
    -e SOCAT_KUBE_CPU_LIMIT=2.0 \
    -e FIELD_SELECTION_WORKSPACES= \
    -e USE_STREAM_CAPABLE_STATE=true \
    -e AIRBYTE_ROLE=dev \
    -e WORKER_ENVIRONMENT=DOCKER \
    -e APPLY_FIELD_SELECTION=false \
    -e WORKER_JOB_ATTEMPT=0 \
    -e OTEL_COLLECTOR_ENDPOINT=http://host.docker.internal:4317 \
    -e FEATURE_FLAG_CLIENT=config \
    -e AIRBYTE_VERSION=0.58.0 \
    -e WORKER_JOB_ID=208afea3-80f7-4648-bc53-ccb08f000a26 \
    public.ecr.aws/l0a4a0e7/source_sapreadtable:latest spec
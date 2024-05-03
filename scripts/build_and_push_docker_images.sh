#!/bin/bash


aws ecr-public get-login-password --profile erpl --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/l0a4a0e7

# Build and push source-sapreadtable
cd source-sapreadtable
docker build --progress plain -t erpl-airbyte/source-sapreadtable:latest .
docker tag erpl-airbyte/source-sapreadtable:latest public.ecr.aws/l0a4a0e7/source_sapreadtable:latest
docker push public.ecr.aws/l0a4a0e7/source_sapreadtable:latest
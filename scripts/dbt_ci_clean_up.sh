#!/bin/bash

set -eu

# on first call bq prints a welcome message to stdout that interferes with jq
bq ls --headless --project_id "$DEV_PROJECT_ID" 1> /dev/null

echo "==> Deleting resources under schema 'ci_*'..."

bq ls --headless --project_id "$DEV_PROJECT_ID" --format=json \
    | jq '.[] | .id | select(. | contains("ci_"))' \
    | xargs -t -n1 bq rm -r -f --dataset

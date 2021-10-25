#!/bin/bash

set -eu

if [[ -z $GITHUB_SHA ]] ; then
    echo "GITHUB_SHA not set! Required to identify schema."
    exit 1
fi

DBT_SCHEMA="ci_${GITHUB_SHA::7}"

# on first call bq prints a welcome message to stdout that interferes with jq
bq ls --headless --project_id "$DEV_PROJECT_ID" 1> /dev/null

echo "==> Deleting resources under schema $DBT_SCHEMA..."

bq ls --headless --project_id "$DEV_PROJECT_ID" --format=json \
    | jq --arg DBT_SCHEMA "$DBT_SCHEMA" '.[] | .id | select(. | contains($DBT_SCHEMA))' \
    | xargs -t -n1 bq rm -r -f --dataset

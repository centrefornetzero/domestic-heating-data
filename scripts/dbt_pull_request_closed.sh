#!/bin/bash

set -eu

if [[ -z $GITHUB_SHA ]] ; then
    echo "GITHUB_SHA not set! Required to identify schema."
    exit 1
fi

bq ls --project_id "$DEV_PROJECT_ID" --format=json \
    | jq --arg SCHEMA "ci_${GITHUB_SHA::7}" '.[] | .id | select(. | contains($SCHEMA))' \
    | xargs -t -n1 bq rm -r -f --dataset

#!/bin/bash

set -eu

if [[ -z $DBT_PROFILES_DIR ]] ; then
    echo "DBT_PROFILES_DIR not set, so dbt will look for profiles in ~/.dbt/"
fi

if [[ -z $GITHUB_SHA ]] ; then
    echo "GITHUB_SHA not set! Required to configure dev dbt profile."
    exit 1
fi

echo "==> Installing dbt dependencies..."
dbt deps --target dev

echo "==> Printing debug info..."
dbt debug --target dev

echo "==> Compiling models and executing against target..."
dbt run --target dev

echo "==> Testing everything except sources..."
dbt test --target dev --exclude "source:*"

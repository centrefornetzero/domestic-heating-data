#!/bin/bash

set -eu

if [[ -z $DBT_PROFILES_DIR ]] ; then
    echo "DBT_PROFILES_DIR not set, so dbt will look for profiles in ~/.dbt/"
fi

echo "==> Installing dbt dependencies..."
dbt deps --target prod

echo "==> Printing debug info..."
dbt debug --target prod

echo "==> Testing sources..."
dbt test --target prod --models "source:*"

echo "==> Compiling models and executing against target..."
dbt run --target prod

echo "==> Testing everything except sources..."
dbt test --target prod --exclude "source:*"

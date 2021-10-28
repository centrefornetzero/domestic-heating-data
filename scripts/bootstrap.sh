#!/bin/bash

set -e

echo "==> Installing Python dependencies..."
pipenv sync --dev || echo "pipenv sync failed, try setting SYSTEM_VERSION_COMPAT=1"

echo "==> Copying dbt profile...."
mkdir -p ~/.dbt || true
cp profiles.yml.template ~/.dbt/profiles.yml

echo "==> Checking connection to data warehouse..."
pipenv run dbt debug --project-dir cnz

echo "Done!"

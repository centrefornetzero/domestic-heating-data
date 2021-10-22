# dbt-data-warehouse

## Installation

```
pipenv sync --dev
```

### Note for macOS 11 (Big Sur) users

Apple changed the version number from 10.X to 11 for Big Sur. This breaks some Python wheels. If you have issues installing, try turning on compatibility mode by setting `SYSTEM_VERSION_COMPAT=1`.

### Create your dbt profile

Create `~/.dbt/profile.yml` and add the following:

```
cnz:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: DEV_PROJECT_ID
      schema: YOUR_INITIALS
      threads: 10
      timeout_seconds: 300
      location: EU
      priority: interactive
      retries: 1
```

Replace `DEV_PROJECT_ID` and `YOUR_INITIALS`, e.g. mine is `twp`.

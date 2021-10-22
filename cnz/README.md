# Data Warehouse Environments

Our production and development environments are in separate Google Cloud projects.

Our sources are loaded into the production environment only. They can still be queried from the development environment. For this to work correctly, every source YAML file must set the `project` property:

```
project: "{{ env_var('PROD_PROJECT_ID') }}"
```

We don't commit the project name to make it easier for other people to re-use the code in the future.

Ideally, we would configure this for all sources in `dbt_project.yml`, but [it's unsupported](https://github.com/dbt-labs/dbt-core/issues/3298) and slated for version 1.0.

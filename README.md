# dbt-data-warehouse

This repo contains the [dbt](https://docs.getdbt.com/) project for the CNZ data warehouse.

Before continuing, read the [introduction in the dbt documentation](https://docs.getdbt.com/docs/introduction).

## Set Up

`cp .env.template .env` and fill in the values in `.env`. Then run `./scripts/bootstrap.sh`.

## dbt Project Structure

Our `models` directory is organised into two folders: `staging` and `marts`.

It is inspired by how [Fishtown Analytics structure their dbt projects](https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355).

### Staging

`staging` is organised by source, e.g. `epc` or `nsul`.
Staging models take raw data, clean it up (fill in/drop missing values, recast types, rename fields, etc.) and make it available for further use.
By doing this we only have to clean up a dataset once.

```
└── models
    └── staging
        ├── nsul
        ├── epc
        ├── ...
    └── marts
```

Within each source folder, you will find:

* `src_<source>.yml`: a source configuration containing documentation and tests for the raw table(s) in the dataset.
* one or more `stg_<source>__<noun>.sql` files: each is a staging model.
* `stg_<source>.yml`: a file containing documentation and tests for each staging model.

If the source requires a lot of transformation, you might also have:

* one or more `base/base_<source>__<noun>.sql` files: intermediate models used by the staging model(s).
* `base.yml`: a file containing documentation and tests for the base models.

### Marts

`marts/` is organised by research project.

```
└── models
    ├── staging
    └── marts
        ├── heat_abm
        ├── transport_abm
        └── transition_dashboard
```

The structure within each project folder is likely to change as we figure things out.
For now, I think a starting point is to build _fact_ and _dimension tables_.

```
└── models
    ├── staging
    └── marts
        └── heat_abm
           ├── dim_households.sql
           ├── fct_heating_upgrade.sql
           └── heat_abm.yml
```

* Fact tables, `fct_<verb>.sql`, are long and narrow and describe immutable events, like upgrading a heating system.
* Dimension tables, `dim_<noun>.sql`, are short and wide and describe things, like households or vehicles.

Both should be documented and tested using `<project>.yml`

Complex models should be broken up into intermediate models in `<project>/intermediate/`.

## Development and Production Environments

We have two data warehouse environments: development and production.
These are separate Google Cloud projects.

Data sources are loaded into the production environment; they can be queried from the development environment.

**The development environment is periodically erased!**

## Development Workflow

1. Write a new model or tests.
2. Run `dbt run` to compile the SQL and execute it agaist the development environment.
3. Run the tests using `dbt test`.
4. Examine the results in [BigQuery](https://console.cloud.google.com/bigquery). All your models will be in datasets prefixed with `<your_initials>_`.


By default dbt will select all models.
You can [select specific models](https://docs.getdbt.com/reference/node-selection/syntax) to speed it up.

To make changes to production, open a pull request.
A Github Action will create the models in the development environment under the schema `ci_<SHORT_GIT_SHA>` and run the tests.
Once the tests have passed and the PR has been reviewed and and merged, a Github Action will be update the models in the production environment.

A GitHub action deletes `ci_*` schemas nightly.

# `domestic-heating-data`

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7322967.svg)](https://doi.org/10.5281/zenodo.7322967)

Data pipelines for [Centre for Net Zero's agent-based model of domestic heating](https://github.com/centrefornetzero/domestic-heating-abm).

The pipelines transform and combine publicly available datasets to produce data relevant to the decisions households in England and Wales make about their heating system.

Read the [post on our tech blog](https://www.centrefornetzero.org/how-we-use-bigquery-dbt-and-github-actions-to-manage-data-at-cnz/) for a longer description of how this works.

## Where can I download the data?

The datasets we use are publicly available but released under their own licences and copyright restrictions.
Here we publish code to transform the datasets.
You need to obtain the datasets yourself and use this code to transform it. The `README` for each dataset in [`cnz/models/staging`](cnz/models/staging) contains a link to download the original data.
If you wish to cite this Github repository that contains the code to transform the datasets, or download the joined dataset (after carefully reading the terms of licenses and restrictions) you can do so by referring to our [Zenodo page for this dataset](https://zenodo.org/record/7322967#.Y3OGWezP16o).

## `dim_household_agents`

[`dim_household_agents`](cnz/models/marts/domestic_heating/dim_household_agents.sql) is the ultimate output of the models.
Each row describes a household we can model in our ABM.

`dim_household_agents` queries [`dim_households`](cnz/models/marts/domestic_heating/dim_households.sql), which contains all the households in `dim_household_agents` _and_ those with insufficient data for us to include in the ABM.


## Supported databases

We use [BigQuery](https://cloud.google.com/bigquery).
We haven't tested it with other databases, but expect that it would work with other databases like PostgreSQL with little or no modifications to the queries.
As of writing there are 168 data tests to help you make any changes with confidence.

## dbt set up

If you are new to dbt, first read the [introduction in the dbt documentation](https://docs.getdbt.com/docs/introduction).

You need Python 3.9 and [`pipenv`](https://github.com/pypa/pipenv).
If you don't have them, see [our instructions for macOS](https://gist.github.com/tomwphillips/715d4fd452ef5d52b4708c0fc5d4f30f).

To set up the project, run:

1. Clone this repo.
2. `cp .env.template .env`.
3. Fill in the values in `.env`.
4. `./scripts/bootstrap.sh`.

Once you've loaded the data into BigQuery you can:

1. Test all your sources: `dbt test --models "source:*"`
2. Run all the models: `dbt run`
3. Test all the models: `dbt test --exclude "source:*"`

If that all succeeded, you should now be able to query `dim_household_agents`.

## CNZ's dbt style guide

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

## Development and production environments

We have two data warehouse environments: development and production.
These are separate Google Cloud projects.

Data sources are loaded into the production environment; they can be queried from the development environment.

**The development environment is periodically erased!**

## Development workflow

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

## CNZ's SQL style guide

We use the [dbt Labs style guide](https://github.com/dbt-labs/corp/blob/master/dbt_style_guide.md).
_Optimize for readability, not lines of code._

Here's the example copy-and-pasted from their guide:

```
with

my_data as (

    select * from {{ ref('my_data') }}

),

some_cte as (

    select * from {{ ref('some_cte') }}

),

some_cte_agg as (

    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5

    from some_cte
    group by 1

),

final as (

    select [distinct]
        my_data.field_1,
        my_data.field_2,
        my_data.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when my_data.cancellation_date is null
                and my_data.expiration_date is not null
                then expiration_date
            when my_data.cancellation_date is null
                then my_data.start_date + 7
            else my_data.cancellation_date
        end as cancellation_date,

        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5

    from my_data
    left join some_cte_agg
        on my_data.id = some_cte_agg.id
    where my_data.field_1 = 'abc'
        and (
            my_data.field_2 = 'def' or
            my_data.field_2 = 'ghi'
        )
    having count(*) > 1

)

select * from final
```

[`SQLFluff`](https://github.com/sqlfluff/sqlfluff) lints models as part of the CI pipeline.
You can run it locally using `sqlfluff lint` and `sqlfluff fix`.

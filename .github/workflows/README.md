# Workflows

| Workflow | Trigger | What it proves |
|---|---|---|
| `ci.yml` / `lint` | every push and PR | `ruff check` and `black --check` over `src`, `tests`, `scripts`, `airflow`. |
| `ci.yml` / `test` | every push and PR | the full pytest suite, including the Spark and Iceberg tests, on a JDK 17 runner. |
| `ci.yml` / `seeds-are-reproducible` | every push and PR | regenerates the dbt seeds from the fixture and fails on any diff, so the committed seeds cannot drift from their generator. |
| `ci.yml` / `dbt` | every push and PR | `dbt deps`, `parse`, `compile`, `seed`, `build` (all models, the snapshot and all 168 tests) against DuckDB, then re-runs one partition twice to assert incremental idempotency. |
| `ci.yml` / `compose` | every push and PR | `docker compose config` for the default stack and all four profiles. |
| `ci.yml` / `airflow-dags` | every push and PR | imports every DAG through `DagBag` and asserts the expected `dag_id`s, so a parse error cannot ship silently. |
| `terraform.yml` / `validate` | `infra/**` changes | `terraform fmt -check`, `init -backend=false`, `validate`, plus a standalone `validate` per module. |
| `terraform.yml` / `tflint` | `infra/**` changes | advisory lint, non-blocking. |

No workflow has AWS credentials and none applies Terraform. `terraform validate`
runs with `-backend=false`, so it checks syntax, types and module wiring without
reaching an account.

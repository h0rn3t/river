# River development

## Run tests

Raise the test database:

    createdb river_test

Run tests:

    make test

By default, this runs all workspace modules and database drivers. Use
`make test/race` to enable the race detector.

To run only the SQLite, libSQL, and Turso driver tests (no Postgres needed):

    make test TEST_DATABASE=sqlite

To run the regular suite with the SQLite and SQL Server driver tests excluded:

    make test TEST_DATABASE=postgres

SQL Server driver tests run against the database in
`TEST_SQLSERVER_DATABASE_URL`, and are skipped when it's not set. To raise a
test database in Docker and run only the SQL Server driver tests:

    docker run -d --name river-sqlserver -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=RiverTest1Password -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
    docker exec river-sqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P RiverTest1Password -Q "CREATE DATABASE river_test; ALTER DATABASE river_test SET READ_COMMITTED_SNAPSHOT ON;"
    export TEST_SQLSERVER_DATABASE_URL="sqlserver://sa:RiverTest1Password@localhost:1433?database=river_test&encrypt=disable"
    make test TEST_DATABASE=sqlserver

`TEST_DATABASE` also applies to `make test/race`. CI separates these suites so
the Postgres version matrix doesn't repeat the SQLite tests. Each suite still
runs on both supported Go versions. Only the latest combination in each matrix
uses the race detector, marked `race` in its job title.

To collect test coverage, set `COVERAGE_DIR` for `make test` or
`make test/race`:

    make test COVERAGE_DIR=coverage
    go tool covdata percent -i=coverage
    go tool covdata textfmt -i=coverage -o coverage.txt

Coverage counts every River package a test runs, so driver code exercised by
the shared suite in `riverdriver/riverdrivertest` counts toward its driver.
Use an empty directory, because data from earlier runs is merged in. In CI,
each suite's race job uploads its coverage to
[Codecov](https://codecov.io/gh/h0rn3t/river) under the `postgres`, `sqlite`,
or `sqlserver` flag, and Codecov merges them. Uploads from this repository
need the Codecov upload token in the `CODECOV_TOKEN` Actions secret.
[`codecov.yml`](../codecov.yml) excludes test infrastructure, development
tooling, and generated sqlc code.

## Run lint

Run the linter and try to autofix:

    golangci-lint run --fix

## Generate sqlc

The project uses sqlc (`brew install sqlc`) to generate Go targets for Postgres
queries. After changing an sqlc `.sql` file, generate Go with:

    make generate

## Raise and migrate development database

To run programs locally outside of tests, create and raise a development database:

    createdb river_dev
    go run ./cmd/river migrate-up --database-url postgres:///river_dev --line main

If needed, override Postgres timeouts for long-running migrations with root CLI
flags:

    go run ./cmd/river --statement-timeout 2m migrate-up --database-url postgres:///river_dev --line main

## Releasing a new version

1. Fetch changes to the repo and any new tags. Export `VERSION` by incrementing the last tag. Execute `update-mod-version` to add it the project's `go.mod` files:

    ```shell
    git checkout master && git pull --rebase
    export VERSION=v0.x.y
    make update-mod-version
    git checkout -b $USER-$VERSION
    ```

2. Prepare a PR with the changes, updating `CHANGELOG.md` with any necessary additions at the same time. Include **`[skip ci]`** in the commit description so that CI doesn't run and pollute the Go Module cache by trying to fetch a version that's not available yet (and it would fail anyway). Have it reviewed and merged.

3. Upon merge, pull down the changes, tag each module with the new version, and push the new tags:

    ```shell
    git checkout master && git pull --rebase
    git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
    git tag riverdriver/$VERSION -m "release riverdriver/$VERSION"
    git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
    git tag riverdriver/riverdatabasesql/$VERSION -m "release riverdriver/riverdatabasesql/$VERSION"
    git tag riverdriver/riversqlite/$VERSION -m "release riverdriver/riversqlite/$VERSION"
    git tag riverdriver/riversqlserver/$VERSION -m "release riverdriver/riversqlserver/$VERSION"
    git tag rivershared/$VERSION -m "release rivershared/$VERSION"
    git tag rivertype/$VERSION -m "release rivertype/$VERSION"
    git tag $VERSION
    ```

4. Push new tags to GitHub:

    ```shell
    git push --tags
    ```

5. Cut a new GitHub release by visiting [new release](https://github.com/h0rn3t/river/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.

### Updating Go or toolchain versions in all `go.mod` files

Modify `go.work` so it contains the new desired version in `go` and/or `toolchain` directives, then run `make update-mod-go` to have it reflect the new version(s) into all the workspace's `go.mod` files:

```shell
make update-mod-go
```

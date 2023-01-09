#  service-java-data-pipelines-metorikku

This is a modified version of [Metorikku](https://github.com/YotpoLtd/metorikku) for **Syngenta Digital** to be ease the task of creating ETLs.

Metorikku is a library that simplifies writing and executing ETLs on top of [Apache Spark](http://spark.apache.org/).

It is based on simple YAML configuration files and runs on any Spark cluster.

The platform also includes a simple way to write unit and E2E tests.

## Changes from Original

- Added compatibility with MongoDB (Input/Output).
- Metrics and Verification results are stored in memory for later used by other apps.
- Metrics files can be read from the same path as the job file using local notation (\"./SOME_PATH"\).
- Optimized to worh with [AWS Glue](https://aws.amazon.com/glue/) 4.0.

## Pre-requisites

1. Docker enabled system:
    - [Docker Desktop (License needed)](https://www.docker.com/products/docker-desktop/)
    - [Rancher Desktop](https://rancherdesktop.io/)

1. The GPG key used in Github:
    - List keys and set UID:

        ```bash
        gpg --list-keys

        # Copy and paste UID key
        KEY_UID=SOME_UID
        ```

    - Export public key:

        ```bash
        # Ensure no passcode is used
        gpg --export -a $KEY_UID > $HOME/.gnupg/public.key
        ```

    - Export private key:

        ```bash
        # Ensure no passcode is used
        gpg --export-secret-key -a $KEY_UID > $HOME/.gnupg/private.key
        ```

## Getting started

To run Metorikku you must first define 2 files.

### Metric file

A metric file defines the steps and queries of the ETL as well as where and what to output.

For example a simple configuration YAML (JSON is also supported) should be as follows:

```yaml
steps:
- dataFrameName: df1
  checkpoint: true #This persists the dataframe to storage and truncates the execution plan. For more details, see https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-checkpointing.html
  sql:
    SELECT *
    FROM input_1
    WHERE id > 100
- dataFrameName: df2
  sql:
    SELECT *
    FROM df1
    WHERE id < 1000
output:
- dataFrameName: df2
  outputType: Parquet
  outputOptions:
    saveMode: Overwrite
    path: df2.parquet
```

You can check out a full example file for all possible values in the [sample YAML configuration file](config/metric_config_sample.yaml).

Make sure to also check out the full [Spark SQL Language manual](https://docs.databricks.com/spark/latest/spark-sql/index.html#sql-language-manual) for the possible queries.

### Job file

This file will include **input sources**, **output destinations** and the location of the **metric config** files.

So for example a simple YAML (JSON is also supported) should be as follows:

```yaml
metrics:
  - /full/path/to/your/metric/file.yaml
inputs:
  input_1: parquet/input_1.parquet
  input_2: parquet/input_2.parquet
output:
    file:
        dir: /path/to/parquet/output
```

You can check out a full example file for all possible values in the [sample YAML configuration file](config/job_config_sample.yaml).

Also make sure to check out:

- All the [tests](tests/data) and [examples](examples).
- Original [project](https://github.com/YotpoLtd/metorikku).

## Git Commit Guidelines

It is obligatory to use the following message format in all the Git commits even with Pull Request:

```git
<type>(<scope>): <subject>
```

Each commit message consists of a **header**, a **body** and a **footer**.  The header has a special format that includes a **type**, a **scope** and a **subject**:

- The **header** is mandatory and the **scope** of the header is optional.
- Any line of the commit message cannot be longer than 100 characters! This allows the message to be easier to read on GitHub as well as in various git tools.

### Types

Must be one of the following:

- **feat**: A new feature.
- **fix**: A bug fix.
- **docs**: Documentation only changes.
- **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc).
- **refactor**: A code change that neither fixes a bug nor adds a feature.
- **perf**: A code change that improves performance.
- **test**: Adding missing or correcting existing tests.
- **chore**: Changes to the build process or auxiliary tools and libraries such as documentation generation.

### Scope

The scope could be anything specifying place of the commit change. For example `jobs`, `utils`, `aws`, etc... You can use `*` when the change affects more than a single scope.

### Subject

The subject contains succinct description of the change:

- Use the imperative, present tense: "change" not "changed" nor "changes".
- Don't capitalize first letter.
- No dot (.) at the end.

## How to publish a new version

The release process works as follow:

- Update version number.
- In case of *mayor* and *minor*, publish a new branch for later patching.
- Upload artifacts to the artifactory server.

[sbt-release](https://github.com/sbt/sbt-release) library is used to generate a new version.

In order to generate a new version:

- Go to the [CircleCI project page](https://app.circleci.com/pipelines/github/syngenta-digital/infra-terraform-data-pipelines).
- Filter the branch. Valid values are**master** or **releases/\***.
- Click in *Trigger pipeline*.
- Add **release_type** parameter with type *string* with one of these values:
    - **patch**: generate a patch verion on **releases/\*** branches.
    - **minor**: generate a minor version on **master** branch.
    - **major** generate a mayor version on **master** branch.
- Click again in *Trigger pipeline* button.

## How to use it

1. Create package

    ```bash
    ./scripts/build.sh
    ```

2. Launch tests

    ```bash
    ./scripts/test.sh
    ```

3. Execute an example

    ```bash
    spark-submit --class com.yotpo.metorikku.Metorikku target/service-java-data-pipelines-metorikku_${SCALA_BINARY_VERSION}*.jar -c examples/movies.yaml
    ```

4. Execute an test example

    ```bash
    spark-submit --class com.yotpo.metorikku.MetorikkuTester target/service-java-data-pipelines-metorikku_${SCALA_BINARY_VERSION}*.jar -t examples/movies_test.yaml
    ```

## Other interesting local commands

1. Access to Postgres (non vscode shell):

    ```bash
    docker exec -it $(docker ps -aqf "name=service-java-data-pipelines-metorikku_devcontainer-postgres-1") bash -c 'psql -U $POSTGRES_USER $POSTGRES_DB'
    ```

2. Access to DocumentDB: (non vscode shell):

    ```bash
    docker exec -it $(docker ps -aqf "name=service-java-data-pipelines-metorikku_devcontainer-mongodb-1") bash -c 'mongo'
    ```

3. Creete package without testing:

    ```bash
    sbt 'set test in assembly := {}' clean assembly
    ```

4. Delete devcontainer containers (non vscode shell):

    ```bash
    docker rm -f $(docker ps -aqf "name=service-java-data-pipelines-metorikku_")
    ```

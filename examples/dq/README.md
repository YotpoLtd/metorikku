![Metorikku Logo](https://raw.githubusercontent.com/wiki/yotpoltd/metorikku/metorikku.png)
## Data Quality
Metorikku offers to add Dataframe validations of top of SQL steps.
The validations are executed on top of [AWSLabs Deequ](https://github.com/awslabs/deequ).
A validation step consists of a `check`, a validation check list, and `level` which define outcome of the validation.
Required params:
* `checks` - validation check list.

Optional params:
* `level` - Log level. Default value `WARN`
* `cacheDf` - Cache all validated dataframes. Default value `true`
```
steps:
- dataFrameName: df1
  sql:
    SELECT col1, col2
    FROM input_1
    WHERE id > 100
  dq:
    level: warn
    checks:
    - isComplete:
        column: col1
    - isComplete:
        column: col2
        level: error
```
### Level
Determines the response of the validation. It's possible to use it as configuration for an entire step or for a single check

`WARN` - Prints validation errors to log.

`ERROR` - Throws exception during execution while causing the execution to fail.
### Checks
Each `checks` list item consists of the operator name as key, and the required paramters for each validation.
#### Operators
The following list includes the supported operators:
##### `isComplete`
Creates a constraint that asserts on a column completion.
Required parameter: `column`
##### `isUnique`
Creates a constraint that asserts on a column uniqueness.
Required parameter: `column`
##### `hasSize`
Creates a constraint that calculates the data frame size and asserts the size according to the given operator defined.
Required parameter: `size`
Required parameter: `operator` Optional values: [==, !=, >=, >, <=, <]
##### `hasUniqueness`
Creates a constraint that asserts on uniqueness in a single or combined set of key columns, according to the given fraction and operator.
Required parameter: `columns`
Optional parameter: `fraction` (Default: "1.0")
Optional parameter: `operator` (Default: "==") Optional values: [==, !=, >=, >, <=, <]
##### `isContainedIn`
Creates a constraint that asserts on a column's allowed values.
Required parameter: `column`
Required parameter: `allowedValues` (List of strings)
##### `satisfies`
Creates a constraint that asserts a given condition on a single column.
Required parameter: `column`
Required parameter: `operator` Valid values: [==, !=, >=, >, <=, <]
Required parameter: `value`

### Failures
To help debug dataframes that failed verification, they will be stored as Parquet files.
To specify a datastore you can use the `CONFIG_FAILED_DF_PATH_PREFIX` environment variable.
If there's no datastore specified - the dataframe will not be saved.


![Metorikku Logo](https://raw.githubusercontent.com/wiki/yotpoltd/metorikku/metorikku.png)
## Data Quality
Metorikku offers to add Dataframe validations of top of SQL steps.
The validations are executed on top of [AWSLabs Deequ](https://github.com/awslabs/deequ).
A validation step consists of a `check`, a validation check list, and `level` which define outcome of the validation.
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
      - op: IsComplete
        column: col1
      - op: IsComplete
        column: col2
        level: error
```
### Level
Determines the response of the validation. It's possible to use it as configuration for an entire step or for a single check

`WARN` - Prints validation errors to log.

`ERROR` - Throws exception during execution while causing the execution to fail.
### Checks
Each `checks` list item consists of `op`, the validation operator, and the required paramters for each validation.
#### Operators
The following list includes the supported operators:
##### `IsComplete`
Creates a constraint that asserts on a column completion.
Required parameters: `column`
##### `IsUnique`
Creates a constraint that asserts on a column uniqueness.
Required parameters: `column`




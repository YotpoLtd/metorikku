* path to run helm commands from
```cd metorikku/helm```
* debugging template
```helm upgrade --dry-run --debug demo-metorikku spark-yotpo --values spark-yotpo/spark-with-hive-built-in.yaml  --values spark-yotpo/values.yaml```
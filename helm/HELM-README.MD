* path to run helm commands from
```cd metorikku/helm```
* debugging template
``` helm upgrade -i --dry-run metorikku-example helm/spark-yotpo/ -f helm/spark-yotpo/custom-values.yaml```
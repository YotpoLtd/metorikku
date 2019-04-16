#!/bin/bash
# wait until MySQL is really available
maxcounter=${MAX_RETRIES}

counter=1
while ! mysql --protocol TCP -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "show databases;" > /dev/null 2>&1; do
    sleep 1
    counter=`expr $counter + 1`
    if [ $counter -gt $maxcounter ]; then
        >&2 echo "We have been waiting for MySQL too long already; failing."
        exit 1
    fi;
done

mysql -h mysql -P 3306 -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" inventory -e "source /scripts/customers_table_updates.sql;"

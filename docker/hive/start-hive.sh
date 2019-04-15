#!/bin/bash

CONNECTION_DRIVER_NAME=${CONNECTION_DRIVER_NAME:=com.mysql.jdbc.Driver}
HIVE_SERVER_PORT=${HIVE_SERVER_PORT:=10000}
SCHEMA_VERIFICATION=${SCHEMA_VERIFICATION:=false}
METASTORE_PORT=${METASTORE_PORT:=9083}
DEFAULT_FS=${DEFAULT_FS:=file:///}
DB_TYPE=${DB_TYPE:=mysql}

cat >${HIVE_HOME}/conf/hive-log4j.properties <<EOL
log4j.rootLogger=INFO, CONSOLE

# Disable excessive reflection warnings - KAFKA-5229
log4j.logger.org.reflections=ERROR

log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.layout=net.logstash.log4j.JSONEventLayoutV1
EOL

cat >${HIVE_HOME}/conf/hive-site.xml <<EOL
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>${CONNECTION_URL}</value>
        <description>JDBC connect string for a JDBC metastore</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>${CONNECTION_DRIVER_NAME}</value>
        <description>Driver class name for a JDBC metastore</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>${CONNECTION_USER_NAME}</value>
        <description>username to use against metastore database</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>${CONNECTION_PASSWORD}</value>
        <description>password to use against metastore database</description>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>${SCHEMA_VERIFICATION}</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>${WAREHOUSE_DIR}</value>
        <description></description>
    </property>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://localhost:${METASTORE_PORT}</value>
    </property>
    <property>
        <name>hive.server2.thrift.port</name>
        <value>${HIVE_SERVER_PORT}</value>
    </property>
     <property>
        <name>fs.default.name</name>
        <value>${DEFAULT_FS}</value>
     </property>
     <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
     </property>
     <property>
        <name>fs.s3.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
     </property>
     <property>
        <name>fs.s3.awsAccessKeyId</name>
        <value>${AWS_ACCESS_KEY}</value>
     </property>
     <property>
        <name>fs.s3.awsSecretAccessKey</name>
        <value>${AWS_SECRET_KEY}</value>
     </property>
</configuration>
EOL

$HIVE_HOME/bin/schematool -dbType ${DB_TYPE} -initSchema

nohup ${HIVE_HOME}/bin/hive --service metastore -p ${METASTORE_PORT} &
${HIVE_HOME}/bin/hiveserver2 --hiveconf hive.root.logger=INFO,console

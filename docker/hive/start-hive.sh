#!/bin/bash

CONNECTION_DRIVER_NAME=${CONNECTION_DRIVER_NAME:=com.mysql.jdbc.Driver}
HIVE_SERVER_PORT=${HIVE_SERVER_PORT:=10000}
SCHEMA_VERIFICATION=${SCHEMA_VERIFICATION:=false}
METASTORE_PORT=${METASTORE_PORT:=9083}
DEFAULT_FS=${DEFAULT_FS:=file:///}
DB_TYPE=${DB_TYPE:=mysql}
USE_ATLAS=${USE_ATLAS:=false}

if [ ! -z ${JSON_LOG} ] ; then
    echo "Setting Log type to JSON"
    cat log4j.json.properties >> ${HIVE_HOME}/conf/hive-log4j.properties
fi

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
        <name>fs.s3n.awsAccessKeyId</name>
        <value>${AWS_ACCESS_KEY}</value>
     </property>
     <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>${AWS_SECRET_KEY}</value>
     </property>
       <property>
        <name>hive.security.authorization.enabled</name>
        <value>false</value>
     </property>
EOL

if [[ ! -z ${USE_ATLAS} ]] ; then
cat >>${HIVE_HOME}/conf/hive-site.xml <<EOL
     <property>
        <name>hive.exec.post.hooks</name>
        <value>org.apache.atlas.hive.hook.HiveHook</value>
     </property>
</configuration>
EOL
# hive-env extra jars
cat >>${HIVE_HOME}/conf/hive-env.sh <<EOL
export HIVE_AUX_JARS_PATH=${ATLAS_HOME}/hook/hive
EOL
# Atlas application properties
cat >${HIVE_HOME}/conf/atlas-application.properties <<EOL
atlas.hook.hive.synchronous=true
atlas.hook.hive.numRetries=3
atlas.hook.hive.queueSize=10000
atlas.cluster.name=primary
atlas.kafka.zookeeper.connection.timeout.ms=30000
atlas.kafka.zookeeper.session.timeout.ms=60000
atlas.kafka.zookeeper.sync.time.ms=20
atlas.kafka.data=${sys:atlas.home}/data/kafka
atlas.kafka.zookeeper.connect=${ZOOKEEPER_CONNECT}
atlas.kafka.bootstrap.servers=${BOOTSTRAP_SERVERS}
atlas.rest.address=${ATLAS_ADDR}
EOL
else
cat >>${HIVE_HOME}/conf/hive-site.xml <<EOL
</configuration>
EOL
fi

$HIVE_HOME/bin/schematool -dbType ${DB_TYPE} -initSchema

nohup ${HIVE_HOME}/bin/hive --service metastore -p ${METASTORE_PORT} &
sleep 10s
${HIVE_HOME}/bin/hiveserver2 --hiveconf hive.root.logger=INFO,console

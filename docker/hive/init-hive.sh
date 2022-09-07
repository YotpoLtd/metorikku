#!/bin/bash

echo "Init Hive"
CONNECTION_DRIVER_NAME=${CONNECTION_DRIVER_NAME:=com.mysql.jdbc.Driver}
HIVE_SERVER_PORT=${HIVE_SERVER_PORT:=10000}
SCHEMA_VERIFICATION=${SCHEMA_VERIFICATION:=false}
METASTORE_PORT=${METASTORE_PORT:=9083}
DEFAULT_FS=${DEFAULT_FS:=file:///}
DB_TYPE=${DB_TYPE:=mysql}
USE_ATLAS=${USE_ATLAS:=false}
HIVE_AUTH=${HIVE_AUTH:=NONE}
KAFKA_LISTENER_TOPIC=${KAFKA_LISTENER_TOPIC:=hive_metastore_listener_events}
MAX_WORKER_THREADS=${MAX_WORKER_THREADS:=2000}
HADOOP_CLIENT_OPTS=${HADOOP_CLIENT_OPTS:='-XX:-UseGCOverheadLimit -Xmx20480m'}

export HADOOP_CLIENT_OPTS=${HADOOP_CLIENT_OPTS}

if [ ! -z ${JSON_LOG} ] ; then
    echo "Setting Log type to JSON"
    cat log4j2.json.properties >> ${HIVE_HOME}/conf/hive-log4j2.properties
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
        <value>thrift://${METASTORE_URI:-localhost}:${METASTORE_PORT:-9083}</value>
    </property>
    <property>
        <name>hive.server2.thrift.port</name>
        <value>${HIVE_SERVER_PORT}</value>
    </property>
    <property>
        <name>hive.server2.authentication</name>
        <value>${HIVE_AUTH}</value>
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
        <name>fs.s3a.access.key</name>
        <value>${AWS_ACCESS_KEY}</value>
     </property>
     <property>
        <name>fs.s3a.secret.key</name>
        <value>${AWS_SECRET_KEY}</value>
     </property>
       <property>
        <name>hive.security.authorization.enabled</name>
        <value>false</value>
     </property>
     <property>
        <name>hive.metastore.disallow.incompatible.col.type.changes</name>
        <value>false</value>
        <description>If true (default is false), ALTER TABLE operations which change the type of   a column (say STRING) to an incompatible type (say MAP&lt;STRING, STRING&gt;) are disallowed.    RCFile default SerDe (ColumnarSerDe) serializes the values in such a way that the  datatypes can be converted from string to any type. The map is also serialized as  a string, which can be read as a string as well. However, with any binary   serialization, this is not true. Blocking the ALTER TABLE prevents ClassCastExceptions  when subsequently trying to access old partitions.   Primitive types like INT, STRING, BIGINT, etc are compatible with each other and are   not blocked.
      See HIVE-4409 for more details.
        </description>
     </property>
     <property>
        <name>hive.async.log.enabled</name>
        <value>false</value>
     </property>
     <property>
        <name>hive.server2.thrift.max.worker.threads</name>
        <value>${MAX_WORKER_THREADS}</value>
     </property>
     <property>
        <name>hive.metastore.server.max.threads</name>
        <value>${MAX_WORKER_THREADS}</value>
     </property>
     <property>
        <name>hive.metastore.metrics.enabled</name>
        <value>true</value>
     </property>
     <property>
        <name>hive.server2.metrics.enabled</name>
        <value>true</value>
     </property>
EOL

if [ $USE_KAFKA_EVENT_LISTENER == 'true' ]; then
cat >>${HIVE_HOME}/conf/hive-site.xml <<EOL
     <property>
        <name>hive.metastore.event.listeners</name>
        <value>com.expediagroup.apiary.extensions.events.metastore.kafka.listener.KafkaMetaStoreEventListener</value>
     </property>
     <property>
        <name>com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.topic.name</name>
        <value>${KAFKA_LISTENER_TOPIC}</value>
     </property>
     <property>
        <name>com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.bootstrap.servers</name>
        <value>${BOOTSTRAP_SERVERS}</value>
     </property>
     <property>
        <name>com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.client.id</name>
        <value>HIVE_CLIENT</value>
     </property>
EOL
fi

if [ $USE_ATLAS == 'true' ]; then
echo "USING ATLAS"
cat >>${HIVE_HOME}/conf/hive-site.xml <<EOL
     <property>
        <name>hive.exec.post.hooks</name>
        <value>org.apache.atlas.hive.hook.HiveHook</value>
     </property>
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
fi

cat >>${HIVE_HOME}/conf/hive-site.xml <<EOL
</configuration>
EOL

$HIVE_HOME/bin/schematool -dbType ${DB_TYPE} -initSchema

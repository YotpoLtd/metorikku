#!/bin/bash

CONNECTION_DRIVER_NAME=${CONNECTION_DRIVER_NAME:=com.mysql.jdbc.Driver}
DB_TYPE=${DB_TYPE:=mysql}

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
		<value>false</value>
	</property>
	<property>
        <name>hive.metastore.warehouse.dir</name>
        <value>${WAREHOUSE_DIR}</value>
        <description></description>
    </property>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://localhost:9083</value>
    </property>
    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>
     <property>
        <name>fs.default.name</name>
        <value>file:///</value>
     </property>
</configuration>
EOL

$HIVE_HOME/bin/schematool -dbType ${DB_TYPE} -initSchema

nohup ${HIVE_HOME}/bin/hive --service metastore &
${HIVE_HOME}/bin/hiveserver2 --hiveconf hive.root.logger=INFO,console

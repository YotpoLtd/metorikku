name := "metorikku"

version := "1.0"

organization := "YotpoLTD"

scalaVersion := "2.11.11"
val jacksonVersion = "2.8.9"
val sparkVersion = "2.2.0"
// | sbt can take a load time checking dependencies. This avoids re-checking the dependencies.
// | Comment this line if error "Skipping update requested, but update has not previously run successfully."
// skip in update := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

libraryDependencies += "com.amazonaws" % "aws-java-sdk" %   "1.11.160"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.4"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.42"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"

libraryDependencies += "RedisLabs" % "spark-redis" % "0.3.2"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.2"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion

libraryDependencies += "com.google.guava" % "guava" % "16.0.1"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.2"

libraryDependencies += "com.databricks" %% "spark-redshift" % "3.0.0-preview1"

libraryDependencies += "com.amazon.redshift" % "redshift-jdbc42" % "1.2.1.1001"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion
)

libraryDependencies += "com.segment.analytics.java" % "analytics" % "2.0.0"

libraryDependencies += "org.influxdb" % "influxdb-java" % "2.7"

// Temporary fix for https://github.com/databricks/spark-redshift/issues/315#issuecomment-285294306
dependencyOverrides += "com.databricks" %% "spark-avro" % "3.2.0"


//Allowing local aws access
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"


javaOptions in Test ++= Seq("-Dspark.master=local[*]")


resolvers += Resolver.sonatypeRepo("public")

resolvers += Resolver.bintrayRepo("spark-packages", "maven")

resolvers += "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"

fork := true

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

javaOptions in Test ++= Seq("-Dspark.master=local[*]")

//Pillar Settings
import io.ino.sbtpillar.Plugin.PillarKeys._
import sbt.Resolver

pillarSettings

pillarConfigFile := file("src/main/resources/application.conf")

pillarConfigKey := "cassandra.url"

pillarReplicationStrategyConfigKey := "cassandra.replicationStrategy"

pillarReplicationFactorConfigKey := "cassandra.replicationFactor"

pillarDefaultConsistencyLevelConfigKey := "cassandra.defaultConsistencyLevel"

pillarMigrationsDir := file("conf/migrations")
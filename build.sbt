name := "metorikku"
organization := "com.yotpo"
homepage := Some(url("https://github.com/YotpoLtd/metorikku"))
licenses := Seq("MIT License" -> url("http://www.opensource.org/licenses/mit-license.html"))
scmInfo := Some(
  ScmInfo(url("https://github.com/YotpoLtd/metorikku"),
    "scm:git:git@github.com:YotpoLtd/metorikku.git"))
developers := List(
  Developer(id="amitco1", name="Amit Cohen", email="", url=url("http://www.yotpo.com")),
  Developer(id="avichay", name="Avichay Etzioni", email="", url=url("http://www.yotpo.com")),
  Developer(id="dporat", name="Doron Porat", email="", url=url("http://www.yotpo.com")),
  Developer(id="etrabelsi", name="Eyal Trabelsi", email="", url=url("http://www.yotpo.com")),
  Developer(id="lyogev", name="Liran Yogev", email="", url=url("http://www.yotpo.com")),
  Developer(id="ofirventura", name="Ofir Ventura", email="", url=url("http://www.yotpo.com")),
  Developer(id="nuriyan", name="Nadav Bar Uriyan", email="", url=url("http://www.yotpo.com")),
  Developer(id="ronbarab", name="Ron Barabash", email="", url=url("http://www.yotpo.com")),
  Developer(id="shirbr", name="Shir Bromberg", email="", url=url("http://www.yotpo.com"))
)

scalaVersion := "2.11.12"
val sparkVersion = Option(System.getProperty("sparkVersion")).getOrElse("2.4.3")
val sparkCassandraVersion = sparkVersion match {
  case "2.4.3" => "2.4.1"
  case "2.4.2" => "2.4.1"
  case "2.2.0" => "2.0.11"
  case other => other
}
val jacksonVersion = "2.9.9"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided" excludeAll(excludeJpountz),
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraVersion,
  "org.apache.avro" % "avro" % "1.8.2",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0" % "test",
  "com.github.scopt" %% "scopt" % "3.6.0",
  "RedisLabs" % "spark-redis" % "0.3.2",
  "org.json4s" %% "json4s-native" % "3.5.2",
  "io.netty" % "netty-all" % "4.1.32.Final",
  "com.google.guava" % "guava" % "16.0.1",
  "com.typesafe.play" %% "play-json" % "2.6.2",
  "com.databricks" %% "spark-redshift" % "3.0.0-preview1",
  "com.amazon.redshift" % "redshift-jdbc42" % "1.2.1.1001",
  "com.segment.analytics.java" % "analytics" % "2.0.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
  "org.scala-lang" % "scala-compiler" % "2.11.12",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
  "com.groupon.dse" % "spark-metrics" % "2.0.0",
  "org.apache.commons" % "commons-text" % "1.6",
  "org.influxdb" % "influxdb-java" % "2.14",
  "org.apache.kafka" %% "kafka" % "2.2.0" % "provided",
  "io.confluent" % "kafka-avro-serializer" % "5.1.2" % "provided",
  "com.uber.hoodie" % "hoodie-spark" % "0.4.7" % "provided",
  "com.uber.hoodie" % "hoodie-common" % "0.4.7" % "provided"
)

// Temporary fix for https://github.com/databricks/spark-redshift/issues/315#issuecomment-285294306
dependencyOverrides += "com.databricks" %% "spark-avro" % "4.0.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala" % jacksonVersion
dependencyOverrides += "org.apache.avro" %% "avro" % "1.8.2"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  Resolver.bintrayRepo("spark-packages", "maven"),
  "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
  "confluent" at "http://packages.confluent.io/maven/"
)

fork := true
javaOptions in Test ++= Seq("-Dspark.master=local[*]")
scalacOptions += "-target:jvm-1.8"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// Assembly settings
Project.inConfig(Test)(baseAssemblySettings)

assemblyMergeStrategy in (Test, assembly) := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case PathList("LICENSE", xs@_*) => MergeStrategy.discard
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
assemblyShadeRules in (Test, assembly) := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
assemblyJarName in assembly := "metorikku.jar"
assemblyJarName in (Test, assembly) := s"${name.value}-standalone.jar"

logLevel in assembly := Level.Error
logLevel in (Test, assembly) := Level.Error

// Publish settings
publishMavenStyle := true

credentials += Credentials("Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("REPO_USER", ""),
  sys.env.getOrElse("REPO_PASSWORD", ""))

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value){
    Opts.resolver.sonatypeSnapshots
  }
  else {
    Opts.resolver.sonatypeStaging
  }
)

pgpPublicRing := baseDirectory.value / "project" / ".gnupg" / "pubring.asc"
pgpSecretRing := baseDirectory.value / "project" / ".gnupg" / "secring.asc"
pgpPassphrase := sys.env.get("PGP_PASS").map(_.toArray)

// Release settings (don't automatically publish upon release)
import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
//  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

// Fix for SBT run to include the provided at runtime
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

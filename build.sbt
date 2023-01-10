organization := "org.syngenta"
name         := "service-java-data-pipelines-metorikku"

homepage := Some(
  url(
    "https://github.com/syngenta-digital/service-java-data-pipelines-metorikku"
  )
)

scmInfo := Some(
  ScmInfo(
    url(
      "https://github.com/syngenta-digital/service-java-data-pipelines-metorikku"
    ),
    "scm:git:git@github.com:syngenta-digital/service-java-data-pipelines-metorikku.git"
  )
)

scalaVersion := Option(System.getenv("SCALA_VERSION")).getOrElse("2.12.17")

val sparkVersion: Def.Initialize[String] = Def.setting {
  Option(System.getenv("SPARK_VERSION")).getOrElse("3.3.1")
}

val jacksonVersion: Def.Initialize[String] = Def.setting {
  Option(System.getenv("JACKSON_VERSION")).getOrElse("2.12.7")
}

val deequVersion: Def.Initialize[String] = Def.setting {
  "2.0.1-spark-3.2"
}

val sparkTestVersion: Def.Initialize[String] = Def.setting {
  "3.3.1_1.3.0"
}

Test / testOptions := Seq(
  Tests.Argument(
    "-l",
    "com.yotpo.metorikku.tags.UnsupportedInCurrentVersion"
  )
)

lazy val excludeJpountz =
  ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
lazy val excludeNetty = ExclusionRule(organization = "io.netty", name = "netty")
lazy val excludeNettyAll =
  ExclusionRule(organization = "io.netty", name = "netty-all")
lazy val excludeAvro =
  ExclusionRule(organization = "org.apache.avro", name = "avro")
lazy val excludeSpark    = ExclusionRule(organization = "org.apache.spark")
lazy val excludeLog4j    = ExclusionRule(organization = "org.apache.logging.log4j")
lazy val excludeParquet  = ExclusionRule(organization = "org.apache.parquet")
lazy val excludeScalanlp = ExclusionRule(organization = "org.scalanlp")
lazy val excludeJacksonCore =
  ExclusionRule(organization = "com.fasterxml.jackson.core")
lazy val excludeJacksonDatatformat =
  ExclusionRule(organization = "com.fasterxml.jackson.dataformat")
lazy val excludeJacksonDatatype =
  ExclusionRule(organization = "com.fasterxml.jackson.datatype")
lazy val excludeJacksonModule =
  ExclusionRule(organization = "com.fasterxml.jackson.module")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql"            % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib"          % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive"           % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-streaming"      % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-avro"           % sparkVersion.value % "provided",
  "com.holdenkarau" %% "spark-testing-base" % sparkTestVersion.value % "test" excludeAll (excludeSpark),
  "com.github.scopt" %% "scopt"         % "3.7.1",
  "org.scala-lang"    % "scala-library" % scalaVersion.value,
  "com.typesafe.play" %% "play-json" % "2.9.3" excludeAll (excludeJacksonCore, excludeJacksonDatatformat, excludeJacksonDatatype, excludeJacksonModule),
  "com.fasterxml.jackson.core"          % "jackson-annotations"       % jacksonVersion.value,
  "com.fasterxml.jackson.core"          % "jackson-core"              % jacksonVersion.value,
  "com.fasterxml.jackson.core"          % "jackson-databind"          % jacksonVersion.value,
  "com.fasterxml.jackson.dataformat"    % "jackson-dataformat-cbor"   % jacksonVersion.value,
  "com.fasterxml.jackson.dataformat"    % "jackson-dataformat-yaml"   % jacksonVersion.value,
  "com.fasterxml.jackson.datatype"      % "jackson-datatype-jdk8"     % jacksonVersion.value,
  "com.fasterxml.jackson.datatype"      % "jackson-datatype-jsr310"   % jacksonVersion.value,
  "com.fasterxml.jackson.module"       %% "jackson-module-scala"      % jacksonVersion.value,
  "org.apache.commons"                  % "commons-text"              % "1.8",
  "org.influxdb"                        % "influxdb-java"             % "2.23",
  "io.github.spark-redshift-community" %% "spark-redshift"            % "5.1.0",
  "com.segment.analytics.java"          % "analytics"                 % "2.1.1" % "provided",
  "com.datastax.spark"                 %% "spark-cassandra-connector" % "3.2.0" % "provided",
  "com.redislabs"                      %% "spark-redis"               % "3.1.0" % "provided",
  "org.apache.kafka"                   %% "kafka"                     % "2.2.0" % "provided",
  "za.co.absa" %% "abris" % "3.2.1" % "provided" excludeAll (excludeAvro, excludeSpark),
  "org.apache.hudi"   %% "hudi-spark-bundle" % "0.10.0" % "provided",
  "org.apache.parquet" % "parquet-avro"      % "1.12.3" % "provided",
  "com.amazon.deequ" % "deequ"     % deequVersion.value excludeAll (excludeSpark, excludeScalanlp),
  "org.apache.avro"  % "avro"      % "1.11.1" % "provided",
  "com.databricks"  %% "spark-xml" % "0.16.0",
  "com.outr"        %% "hasher"    % "1.2.2",
  "org.mongodb.spark"       %% "mongo-spark-connector" % "10.1.0",
  "mysql"                    % "mysql-connector-java"  % "8.0.31" % "provided",
  "org.apache.logging.log4j" % "log4j-api"             % "2.19.0" % "provided",
  "org.apache.logging.log4j" % "log4j-core"            % "2.19.0" % "provided",
  "org.apache.logging.log4j" % "log4j-slf4j-impl"      % "2.19.0" % "provided",
  "org.postgresql"           % "postgresql"            % "42.5.1" % "provided"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "confluent" at "https://packages.confluent.io/maven/"
)

fork := true

Test / javaOptions ++= Seq(
  "-Dspark.master=local[*]",
  "-Dspark.sql.session.timeZone=UTC",
  "-Duser.timezone=UTC"
)

// Assembly settings
Project.inConfig(Test)(baseAssemblySettings)

assembly / assemblyMergeStrategy := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case PathList("LICENSE", xs @ _*)               => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "log4j.properties"            => MergeStrategy.first
  case _                             => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
assembly / assemblyOption := (assembly / assemblyOption).value
  .copy(cacheOutput = false)
assembly / assemblyOption := (assembly / assemblyOption).value
  .copy(cacheUnzip = false)
assembly / logLevel := Level.Error

// Publish settings
publishMavenStyle := true

// publishTo := Some("fury" at "https://maven.fury.io/syngenta-digital/")
// credentials += Credentials(
//   "fury",
//   "maven.fury.io",
//   sys.env.getOrElse("FURY_AUTH", ""),
//   "NOPASS"
// )

publishTo := Some(
  "GitHub Package Registry" at "https://maven.pkg.github.com/syngenta-digital/service-java-data-pipelines-metorikku"
)
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_EMAIL", ""),
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

ThisBuild / versionScheme := Some("early-semver")

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

Compile / assembly / artifact := {
  val art = (Compile / assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(Compile / assembly / artifact, assembly)

// Fix for SBT run to include the provided at runtime
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

releaseNextCommitMessage := s"Setting version to ${(ThisBuild / version).value} [skip ci]"

commands += Command.command("bump-patch") { state =>
  println("Bumping patch version!")
  val extracted = Project extract state
  val customState = extracted.appendWithoutSession(
    Seq(
      releaseVersionBump := sbtrelease.Version.Bump.Bugfix
    ),
    state
  )
  Command.process("release with-defaults", customState)
}

commands += Command.command("bump-minor") { state =>
  println("Bumping minor version!")
  val extracted = Project extract state
  val customState = extracted.appendWithoutSession(
    Seq(
      releaseVersionBump := sbtrelease.Version.Bump.Minor
    ),
    state
  )
  Command.process("release with-defaults", customState)
}

commands += Command.command("bump-major") { state =>
  println("Bumping major version!")
  val extracted = Project extract state
  val customState = extracted.appendWithoutSession(
    Seq(
      releaseVersionBump := sbtrelease.Version.Bump.Major
    ),
    state
  )
  Command.process("release with-defaults", customState)
}

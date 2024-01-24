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

val sparkTestVersion: Def.Initialize[String] = Def.setting {
  "3.3.1_1.3.0"
}

// sbt-scalafix
semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision
scalacOptions += "-Ywarn-unused-import"

Test / testOptions := Seq(
  Tests.Argument(
    "-l",
    "com.yotpo.metorikku.tags.UnsupportedInCurrentVersion"
  )
)

lazy val excludeAvro     = ExclusionRule(organization = "org.apache.avro", name = "avro")
lazy val excludeSpark    = ExclusionRule(organization = "org.apache.spark")
lazy val excludeLog4j    = ExclusionRule(organization = "org.apache.logging.log4j")
lazy val excludeParquet  = ExclusionRule(organization = "org.apache.parquet")
lazy val excludeScalanlp = ExclusionRule(organization = "org.scalanlp")
lazy val excludeJackson  = ExclusionRule(organization = "com.fasterxml.jackson.*")
lazy val excludeAWS      = ExclusionRule(organization = "com.amazonaws")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql"            % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib"          % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive"           % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-streaming"      % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-avro"           % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion.value % "provided" excludeAll (excludeAWS),
  "com.holdenkarau" %% "spark-testing-base" % sparkTestVersion.value % "test" excludeAll (excludeSpark),
  "com.github.scopt"          %% "scopt"               % "3.7.1",
  "org.scala-lang"             % "scala-library"       % scalaVersion.value,
  "com.typesafe.play"         %% "play-json"           % "2.9.4" excludeAll (excludeJackson),
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion.value,
  "com.fasterxml.jackson.core" % "jackson-core"        % jacksonVersion.value,
  "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonVersion.value,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion.value,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion.value,
  "com.fasterxml.jackson.datatype"   % "jackson-datatype-jdk8"   % jacksonVersion.value,
  "com.fasterxml.jackson.datatype"   % "jackson-datatype-jsr310" % jacksonVersion.value,
  "com.fasterxml.jackson.module"    %% "jackson-module-scala"    % jacksonVersion.value,
  "com.hubspot.jinjava"                 % "jinjava"        % "2.7.1" excludeAll (excludeJackson),
  "org.influxdb"                        % "influxdb-java"  % "2.23",
  "io.github.spark-redshift-community" %% "spark-redshift" % "6.2.0-spark_3.3" excludeAll (excludeAWS),
  "com.segment.analytics.java"          % "analytics"      % "2.1.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1" % "provided",
  "com.redislabs"      %% "spark-redis"               % "3.1.0" % "provided",
  "org.apache.kafka"   %% "kafka"                     % "3.6.1" % "provided",
  "za.co.absa" %% "abris" % "3.2.1" % "provided" excludeAll (excludeAvro, excludeSpark),
  "org.apache.hudi"   %% "hudi-spark-bundle" % "0.10.0" % "provided",
  "org.apache.parquet" % "parquet-avro"      % "1.12.3" % "provided",
  "com.amazon.deequ" % "deequ"     % "2.0.4-spark-3.3" excludeAll (excludeSpark, excludeScalanlp),
  "org.apache.avro"  % "avro"      % "1.11.1" % "provided",
  "com.databricks"  %% "spark-xml" % "0.17.0",
  "com.outr"        %% "hasher"    % "1.2.2",
  "org.mongodb.spark"       %% "mongo-spark-connector"     % "10.2.1",
  "mysql"                    % "mysql-connector-java"      % "8.0.33" % "provided",
  "org.apache.logging.log4j" % "log4j-api"                 % "2.22.1" % "provided",
  "org.apache.logging.log4j" % "log4j-core"                % "2.22.1" % "provided",
  "org.apache.logging.log4j" % "log4j-slf4j-impl"          % "2.22.1" % "provided",
  "org.postgresql"           % "postgresql"                % "42.7.1" % "provided",
  "io.delta"                %% "delta-core"                % "2.3.0",
  "io.vertx"                 % "vertx-json-schema"         % "4.5.1" excludeAll (excludeJackson),
  "com.google.guava"         % "guava"                     % "25.0-jre",
  "org.apache.sedona"       %% "sedona-core-3.0"           % "1.4.1" excludeAll (excludeSpark),
  "org.apache.sedona"       %% "sedona-sql-3.0"            % "1.4.1" excludeAll (excludeSpark),
  "org.apache.sedona"       %% "sedona-viz-3.0"            % "1.4.1" excludeAll (excludeSpark),
  "org.datasyslab"           % "geotools-wrapper"          % "1.4.0-28.2" excludeAll (excludeSpark),
  "com.amazonaws"            % "aws-java-sdk-s3"           % "1.12.642",
  "software.amazon.awssdk"   % "dynamodb"                  % "2.23.8",
  "software.amazon.awssdk"   % "glue"                      % "2.23.8",
  "software.amazon.awssdk"   % "s3"                        % "2.23.8",
  "software.amazon.awssdk"   % "sts"                       % "2.23.8",
  "org.apache.iceberg"      %% "iceberg-spark-runtime-3.3" % "1.4.3",
  "com.jayway.jsonpath"      % "json-path"                 % "2.8.0"
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
  case PathList("log4j.properties")  => MergeStrategy.first
  case PathList("hive-site.xml")     => MergeStrategy.discard
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

releaseCommitMessage := s"Setting version to ${(ThisBuild / version).value} [skip ci]"

commands += Command.command("bump-patch") { state =>
  val extracted = Project extract state
  val customState = extracted.appendWithoutSession(
    Seq(
      releaseVersion := { ver =>
        sbtrelease
          .Version(ver)
          .map(_.bump(sbtrelease.Version.Bump.Bugfix).string)
          .getOrElse(sbtrelease.versionFormatError(ver))
      }
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
      releaseVersion := { ver =>
        sbtrelease
          .Version(ver)
          .map(_.bump(sbtrelease.Version.Bump.Minor).string)
          .getOrElse(sbtrelease.versionFormatError(ver))
      }
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
      releaseVersion := { ver =>
        sbtrelease
          .Version(ver)
          .map(_.bump(sbtrelease.Version.Bump.Major).string)
          .getOrElse(sbtrelease.versionFormatError(ver))
      }
    ),
    state
  )
  Command.process("release with-defaults", customState)
}

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  pushChanges
)

name := "metorikku"
organization := "com.yotpo"
homepage := Some(url("https://github.com/YotpoLtd/metorikku"))
licenses := Seq("MIT License" -> url("http://www.opensource.org/licenses/mit-license.html"))
scmInfo := Some(
  ScmInfo(url("https://github.com/YotpoLtd/metorikku"),
    "scm:git:git@github.com:YotpoLtd/metorikku.git"))
developers := List(
  Developer(id="Yotpo", name="Yotpo", email="", url=url("http://www.yotpo.com"))
)

crossScalaVersions := Seq(
  Option(System.getenv("SCALA_VERSION")).getOrElse("2.12.15"),
  Option(System.getenv("SPARK2_SCALA_VERSION")).getOrElse("2.11.12"))
scalaVersion := Option(System.getenv("SCALA_VERSION")).getOrElse("2.12.15")

val sparkVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => Option(System.getenv("SPARK_VERSION")).getOrElse("3.2.0")
    case _ => Option(System.getenv("SPARK2_VERSION")).getOrElse("2.4.6")
  }
}

val jacksonVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => Option(System.getenv("JACKSON_VERSION")).getOrElse("2.10.0")
    case _ => Option(System.getenv("SPARK2_JACKSON_VERSION")).getOrElse("2.9.9")
  }
}

val sparkRedshiftVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => "4.2.0"
    case _ => "4.1.1"
  }
}

val parquetVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => "1.12.1"
    case _ => "1.10.1"
  }
}

val deequVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => "2.0.0-spark-3.1"
    case _ => "1.1.0_spark-2.4-scala-2.11"
  }
}

val sparkTestVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => "3.2.0_1.1.1"
    case _ => "2.4.5_0.14.0"
  }
}

testOptions in Test := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => Seq(Tests.Argument("-l","com.yotpo.metorikku.tags.UnsupportedInCurrentVersion"))
    case _ => Seq()
  }
}

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
lazy val excludeNetty = ExclusionRule(organization = "io.netty", name = "netty")
lazy val excludeNettyAll = ExclusionRule(organization = "io.netty", name = "netty-all")
lazy val excludeAvro = ExclusionRule(organization = "org.apache.avro", name = "avro")
lazy val excludeSpark = ExclusionRule(organization = "org.apache.spark")
lazy val excludeLog4j = ExclusionRule(organization = "org.apache.logging.log4j")
lazy val excludeParquet = ExclusionRule(organization = "org.apache.parquet")
lazy val excludeScalanlp = ExclusionRule(organization = "org.scalanlp")


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion.value % "provided",

  "com.holdenkarau" %% "spark-testing-base" % sparkTestVersion.value % "test" excludeAll excludeSpark,

  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion.value,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion.value,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion.value,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion.value,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion.value,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion.value,
  "org.apache.commons" % "commons-text" % "1.8",
  "org.influxdb" % "influxdb-java" % "2.14",
  "io.github.spark-redshift-community" %% "spark-redshift" % sparkRedshiftVersion.value,
  "com.segment.analytics.java" % "analytics" % "2.1.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0-alpha2" % "provided",
  "com.redislabs" %% "spark-redis" % "2.5.0" % "provided",
  "org.apache.kafka" %% "kafka" % "2.2.0" % "provided",
  "za.co.absa" %% "abris" % "3.2.1"  % "provided" excludeAll(excludeAvro, excludeSpark),
  "org.apache.hudi" %% "hudi-spark-bundle" % "0.10.0" % "provided",
  "org.apache.parquet" % "parquet-avro" % parquetVersion.value % "provided",
  "com.amazon.deequ" % "deequ" % deequVersion.value excludeAll(excludeSpark, excludeScalanlp),
  "org.apache.avro" % "avro" % "1.8.2" % "provided",
  "com.databricks" %% "spark-xml" % "0.11.0",
  "com.outr" %% "hasher" % "1.2.2"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  Resolver.bintrayRepo("spark-packages", "maven"),
  "confluent" at "https://packages.confluent.io/maven/"
)

fork := true

javaOptions in Test ++= Seq("-Dspark.master=local[*]", "-Dspark.sql.session.timeZone=UTC", "-Duser.timezone=UTC")


// Assembly settings
Project.inConfig(Test)(baseAssemblySettings)

assemblyMergeStrategy in (Test, assembly) := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case PathList("LICENSE", xs@_*) => MergeStrategy.discard
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.first
  case _ => MergeStrategy.first
}

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case PathList("LICENSE", xs@_*) => MergeStrategy.discard
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.first
  case _ => MergeStrategy.first
}

assemblyShadeRules in (Test, assembly) := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)
assemblyJarName in assembly := s"metorikku_${scalaBinaryVersion.value}.jar"
assemblyJarName in (Test, assembly) := s"${name.value}-standalone_${scalaBinaryVersion.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)
assemblyOption in (Test, assembly) := (assemblyOption in (Test, assembly)).value.copy(cacheOutput = false)
assemblyOption in (Test, assembly) := (assemblyOption in (Test, assembly)).value.copy(cacheUnzip = false)

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

useGpg := false
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

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

artifact in (Test, assembly) := {
  val art = (artifact in (Test, assembly)).value
  art.withClassifier(Some("standalone"))
}

addArtifact(artifact in (Test, assembly), assembly in Test)

// Fix for SBT run to include the provided at runtime
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

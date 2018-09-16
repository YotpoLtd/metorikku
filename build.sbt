name := "metorikku"
organization := "com.yotpo"
homepage := Some(url("https://github.com/YotpoLtd/metorikku"))
licenses := Seq("MIT License" -> url("http://www.opensource.org/licenses/mit-license.html"))
scmInfo := Some(
  ScmInfo(url("https://github.com/YotpoLtd/metorikku"),
    "scm:git:git@github.com:YotpoLtd/metorikku.git"))
developers := List(
  Developer(id="amitco1", name="Amit Cohen", email="amit@yotpo.com", url=url("http://www.yotpo.com")),
  Developer(id="avichay", name="Avichay Etzioni", email="avichay@yotpo.com", url=url("http://www.yotpo.com")),
  Developer(id="etrabelsi", name="Eyal Trabelsi", email="etrabelsi@yotpo.com", url=url("http://www.yotpo.com")),
  Developer(id="lyogev", name="Liran Yogev", email="lyogev@yotpo.com", url=url("http://www.yotpo.com")),
  Developer(id="ofirventura", name="Ofir Ventura", email="oventura@yotpo.com", url=url("http://www.yotpo.com")),
  Developer(id="ronbarab", name="Ron Barabash", email="rbarabash@yotpo.com", url=url("http://www.yotpo.com")),
  Developer(id="shirbr", name="Shir Bromberg", email="sbromberg@yotpo.com", url=url("http://www.yotpo.com"))
)

scalaVersion := "2.11.11"
val jacksonVersion = "2.8.9"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.4" % "test",
  "com.github.scopt" %% "scopt" % "3.6.0",
  "RedisLabs" % "spark-redis" % "0.3.2",
  "org.json4s" %% "json4s-native" % "3.5.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3",
  "com.google.guava" % "guava" % "16.0.1",
  "com.typesafe.play" %% "play-json" % "2.6.2",
  "com.databricks" %% "spark-redshift" % "3.0.0-preview1",
  "com.amazon.redshift" % "redshift-jdbc42" % "1.2.1.1001",
  "com.segment.analytics.java" % "analytics" % "2.0.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
  "com.groupon.dse" % "spark-metrics" % "2.0.0"
)

// Temporary fix for https://github.com/databricks/spark-redshift/issues/315#issuecomment-285294306
dependencyOverrides += "com.databricks" %% "spark-avro" % "4.0.0"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  Resolver.bintrayRepo("spark-packages", "maven"),
  "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
)

fork := true
//TODO(etrabelsi@yotpo.com) doesnt work
javaOptions in Test ++= Seq("-Dspark.master=local[*]")

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

// Publish settings
publishMavenStyle := true

credentials += Credentials("Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("REPO_USER", ""),
  sys.env.getOrElse("REPO_PASSWORD", ""))

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
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

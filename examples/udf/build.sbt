name := "udf-example"

version := "1.0"

scalaVersion := Option(System.getProperty("scalaVersion")).getOrElse("2.12.17")

val sparkVersion: Def.Initialize[String] = Def.setting {
  Option(System.getenv("SPARK_VERSION")).getOrElse("3.1.3")
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"
)

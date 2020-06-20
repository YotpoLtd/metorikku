name := "udf-example"

version := "1.0"

crossScalaVersions := Seq("2.12.11", "2.11.12")
scalaVersion := Option(System.getProperty("scalaVersion")).getOrElse("2.12.11")

val sparkVersion: Def.Initialize[String] = Def.setting {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 12 => Option(System.getProperty("sparkVersion")).getOrElse("3.0.0")
    case _ => "2.4.5"
  }
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"
)
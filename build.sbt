
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "toy-ranking",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.scalatest" %% "scalatest" % "3.2.16" % Test,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.2"
       )
  )
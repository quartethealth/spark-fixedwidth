name := "spark-fixedwidth"

version := "1.0"

organization := "com.quartethealth"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.univocity" % "univocity-parsers" % "1.5.1",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
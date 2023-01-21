ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "codingtest_sapient_202301"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test

val sparkVersion = "3.2.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.5"

lazy val root = (project in file("."))
  .settings(
    name := "CENG790-Big Data Analytics-DataFrame API and RDDs"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"

name := "DataTransformation2"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "net.liftweb" %% "lift-json" % "3.0-M1")

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.3.0"
    
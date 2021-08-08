import scala.tools.nsc.Reporting.Version

name := "Assignment"

version := "0.1"

scalaVersion := "2.11.12"

name := "IntelliJSparkDemo"
scalaVersion := "2.11.12"
//libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.1.0")

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  //"org.scalactic" %% "scalactic" % scalaVersion,
  //"org.scalatest" %% "scalatest" % scalaVersion % "test",
   //"org.apache.spark" %% "spark-core" % scalaVersion % Test classifier "tests",
  // "org.apache.spark" %% "spark-sql" % scalaVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided, // as usual
  "org.apache.spark" %% "spark-core" % sparkVersion % Test, // as usual
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests", // to make magic working

  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",


)
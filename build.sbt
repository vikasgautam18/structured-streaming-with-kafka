name := "structured-streaming-with-kafka"

version := "0.1"

scalaVersion := "2.11.8"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
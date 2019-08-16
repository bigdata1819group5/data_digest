name := "data_digest"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.1"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.1"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
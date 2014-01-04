name := "hbase-getting-started"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.3"

organization := "org.littlewings"

scalacOptions ++= Seq("-Xlint", "-deprecation")

resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.5.0",
  "org.apache.hadoop" % "hadoop-annotations" % "2.0.0-cdh4.5.0",
  "org.apache.hbase" % "hbase" % "0.94.6-cdh4.5.0",
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)

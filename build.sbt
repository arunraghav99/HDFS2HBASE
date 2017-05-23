name := "HDFS2HBASE"

version := "1.0"

scalaVersion := "2.12.2"


libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-common" % "1.2.1" % "provided",
  "org.apache.hbase" % "hbase-client" % "1.2.1" % "provided",
  "org.apache.hbase" % "hbase-server" % "1.2.1" % "provided",
  "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
)

resolvers ++= Seq(
  "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/"
)
    
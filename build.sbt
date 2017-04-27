name := "bubblical"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion: String = "2.1.0"
val akkaVersion: String = "10.0.3"
val sparkTestingVersion: String  = sparkVersion + "_0.3.3"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe.akka" %% "akka-http" % akkaVersion ,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaVersion,
  "junit" % "junit" % "4.10" % "test",
//  "org.scalactic" %% "scalactic" % "3.0.0",
//  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
//  "com.holdenkarau" %% "spark-testing-base" % sparkTestingVersion ,
//  "org.scalaj" % "scalaj-time_2.10.2" % "0.7",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
//  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5",
  "mysql" % "mysql-connector-java" % "5.1.33"
)

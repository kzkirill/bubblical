name := "bubblical"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion: String = "2.1.0"
val akkaVersion: String = "10.0.3"
val sparkTestingVersion: String  = sparkVersion + "_0.3.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe.akka" %% "akka-http" % akkaVersion ,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaVersion,
  "org.scalactic" %% "scalactic" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
//  "com.holdenkarau" %% "spark-testing-base" % sparkTestingVersion ,
  "mysql" % "mysql-connector-java" % "5.1.33"
)

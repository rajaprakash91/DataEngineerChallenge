name := "DataEngineerChallenge"
organization := "raja.paypay"
version := "0.1"
scalaVersion := "2.12.10"

autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"
val scalaTest = "3.2.9"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTest % "test"
)

libraryDependencies ++= sparkDependencies

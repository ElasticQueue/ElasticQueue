import AssemblyKeys._
import spray.revolver.RevolverPlugin._

assemblySettings

name := "mq"

version := "0.1"

scalaVersion := "2.11.6"

val akkaVersion = "2.3.6"

resolvers ++= Seq(
  "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo" at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging" at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "org.json4s" %% "json4s-ext" % "3.2.10",
  "io.spray" %% "spray-can" % "1.3.2",
  "io.spray" %% "spray-routing" % "1.3.2",
  "io.spray" %% "spray-json" % "1.3.1",
  "io.spray" %% "spray-client" % "1.3.2",
  "joda-time"  % "joda-time" % "2.4",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

Revolver.settings

//lazy val `wtb-spark` = project.settings(assemblySettings: _*)

//lazy val client = project.dependsOn(common).settings(assemblySettings: _*)


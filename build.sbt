import AssemblyKeys._

assemblySettings

name := "mq"

version := "0.1"

scalaVersion := "2.11.5"

val phantomVersion = "1.5.0"
val akkaVersion = "2.3.6"

resolvers ++= Seq(
  "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo" at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging" at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository" at "http://download.java.net/maven/2/",
  "Twitter Repository" at "http://maven.twttr.com",
  "Websudos releases" at "http://maven.websudos.co.uk/ext-release-local"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  ("com.typesafe.akka" %% "akka-contrib" % akkaVersion).
  exclude("org.iq80.leveldb", "leveldb-api"),
  "com.websudos" %% "phantom-dsl" % phantomVersion,
  ("com.websudos" %% "phantom-zookeeper" % phantomVersion).
    exclude("com.twitter.common.zookeeper", "server-set"),
  "io.spray" %% "spray-can" % "1.3.2",
  "io.spray" %% "spray-routing" % "1.3.2",
  "io.spray" %% "spray-json" % "1.3.1",
  "io.spray" %% "spray-client" % "1.3.2"
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

//lazy val `wtb-spark` = project.settings(assemblySettings: _*)

//lazy val client = project.dependsOn(common).settings(assemblySettings: _*)


import scalariform.formatter.preferences._

name := """flight-delay-streaming-scala"""

version := "1.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-stream_2.11" % "2.4.2",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.4.2"
)



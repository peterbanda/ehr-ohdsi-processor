organization := "com.bnd"

name := "ehr-ohdsi-processor"

version := "0.0.2"

scalaVersion := "2.12.10" ///"2.11.12"

resolvers ++= Seq(
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.26", // 2.4.17",
  "commons-io" % "commons-io" % "2.6",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

mainClass in assembly := Some("com.bnd.ehrop.CalcFeatures")

assemblyMergeStrategy in assembly := {
//  case PathList("versionchanges.txt") => MergeStrategy.discard
//  case PathList("tessdata/") => MergeStrategy.discard
  case PathList("org", "joda", "time", "base", "BaseDateTime.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

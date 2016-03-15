name := "VarDictSpark"
organization := "md.fusionworks"

version := "0.1"

scalaVersion := "2.10.4"

//uncomment this to use `sbt dependencies`
/*
import play.PlayImport.PlayKeys._

lazy val pr = (project in file(".")).enablePlugins(PlayScala)
*/
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

//javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")


libraryDependencies ++= adamDependencies ++ sparkDependencies ++ varDictDependencies

lazy val adamDependencies = Seq("org.bdgenomics.adam" %% "adam-core" % "0.19.0"/* % "provided"*/)

lazy val varDictDependencies = Seq(
  "com.edropple.jregex" % "jregex" % "1.2_01"
)


lazy val sparkDependencies = {
  val sparkV = "1.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV % "provided" ,
    "org.apache.spark" %% "spark-sql" % sparkV % "provided"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case PathList(ps@_*) if ps.last endsWith "reference.conf" => MergeStrategy.concat
  case PathList(ps@_*) if ps.last endsWith "_SUCCESS" => MergeStrategy.discard
  case PathList(ps@_*) if ps.last endsWith ".parquet" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

test in assembly := {}

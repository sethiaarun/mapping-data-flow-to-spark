import sbtassembly.MergeStrategy

name := "mdf-to-spark"
version := "0.1"
scalaVersion := "2.13.11"

lazy val app = (project in file("app"))
  .settings(
    assembly / mainClass := Some("com.microsoft.azure.adf.tool.MdfToSpark"),
    assembly / assemblyJarName := "mdftospark.jar"
  )


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.concat
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case PathList("reflect.properties") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
  "org.scalactic" %% "scalactic" % "3.2.16",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "org.apache.commons" % "commons-text" % "1.10.0",
  "org.scalameta" %% "scalafmt-dynamic" % "3.7.11",
  "black.ninia" % "jep" % "4.1.1",
  "com.azure.resourcemanager" % "azure-resourcemanager-datafactory" % "1.0.0-beta.22",
  "com.azure" % "azure-identity" % "1.10.0",
  "org.scalatest" %% "scalatest" % "3.2.16" % "test"
)


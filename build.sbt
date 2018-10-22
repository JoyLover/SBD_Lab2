scalaVersion := "2.11.12"

lazy val lab = (project in file("."))
  .settings(
    name := "lab_2",
    fork in run := true,

    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"%"provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"%"provided",
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"%"provided",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.0"%"provided"
  )
  
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
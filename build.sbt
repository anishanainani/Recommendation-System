name := "Recommendation system"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

fork in run := true
mainClass in (Compile,run) := Some("org.apache.spark.recommendation.Recommendation")

name := "Spark-Count-By-Year"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" )





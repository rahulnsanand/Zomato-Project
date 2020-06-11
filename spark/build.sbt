crossPaths:=false

name := "Module1"
version := "1.0"
scalaVersion := "2.11.8"

//Default libraryDependencies for spark sql as well as hadoop operations
libraryDependencies +="org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"

//ColoredFontFunctionality
libraryDependencies +="org.backuity" %% "ansi-interpolator" % "1.1" % "provided"

//LoggingFunctionality
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "log4j" % "apache-log4j-extras" % "1.1"


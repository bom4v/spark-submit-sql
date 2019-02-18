name := "sql-to-csv-spark"

organization := "org.bom4v.ti"

version := "0.0.1"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.6", "2.11.12")

checksums in update := Nil

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.4.1" % "test"

// Spark
val sparkVersion = "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snaspshots")
)
// "Local repository"     at "http://localhost/mavenrepo/",
// Resolver.mavenLocal

javacOptions in Compile ++= Seq("-source", "1.8",  "-target", "1.8")

scalacOptions ++= Seq("-deprecation", "-feature")

publishTo := Some("Local Maven Repo" at "http://localhost/mavenrepo/")

cleanKeepFiles += target.value / "test-reports"



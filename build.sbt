name := "sql-to-csv-spark"

organization := "org.bom4v.ti"

organizationName := "Business Object Models for Verticals (BOM4V)"

organizationHomepage := Some(url("http://github.com/bom4v"))

version := "0.0.1"

homepage := Some(url("https://github.com/bom4v/spark-submit-sql"))

startYear := Some(2019)

description := "From SQL queries to CSV files with native Spark jobs (in Scala and Python)"

licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")

useGpg := true

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

pomExtra := (
  <scm>
    <url>https://github.com/bom4v/spark-submit-sql/tree/master</url>
    <connection>scm:git:git://github.com/bom4v/spark-submit-sql.git</connection>
    <developerConnection>scm:git:ssh://github.com:bom4v/spark-submit-sql.git</developerConnection>
  </scm>
)

cleanKeepFiles += target.value / "test-reports"



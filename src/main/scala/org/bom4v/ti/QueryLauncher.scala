package org.bom4v.ti

//import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
  * Spark job aimed at being launched in a standalone mode,
  * for instance within the SBT JVM
  */
object StandaloneQueryLauncher extends App {
  //
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("StandaloneQuerylauncher")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  //
  Utilities.displayVersions (spark)

  //
  val defaultQueryFile = "requests/hive-sql-to-csv-01-test.sql"
  val defaultCSVFile = "hive-generic.csv"

  // Retrieve the filename of the SQL query, if given as command line parameter
  val queryFile = Utilities.getQueryFilePath (defaultQueryFile, args)
  println ("File-path for the SQL query: " + queryFile)

  // Retrieve the expected filename of the resulting CSV file,
  // if given as command line parameter
  val outputCSVFile = Utilities.getOutputCSVFilePath (defaultCSVFile, args)
  println ("File-path for the expected CSV file: " + outputCSVFile)

  // Extract the SQL query from the given file
  val sqlQuery = Utilities.extractQuery (queryFile)
  println ("SQL query: " + sqlQuery)

  // End of the Spark session
  spark.stop()
}

/**
  * Spark job aimed at being launched on a Spark cluster,
  * typically with YARN or Mesos
  */
object SparkClusterQueryLauncher extends App {
  //
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("SparkClusterQueryLauncher")
    .enableHiveSupport()
    .config("hive.exec.compress.output", "false")
    .getOrCreate()

  //
  Utilities.displayVersions (spark)

  // CSV data file, from HDFS
  val hdfsDataDir = "incoming"
  val defaultCSVFilename = "hive-generic.csv"

  // File in which the SQL query is specified
  val defaultQueryFile = "requests/hive-sql-to-csv-01-test.sql"

  // Temporary Hive table name
  val hiveTempTable = "hive_generic"

  // Retrieve the filename of the SQL query, if given as command line parameter
  val queryFile = Utilities.getQueryFilePath (defaultQueryFile, args)
  println ("File-path for the SQL query: " + queryFile)

  // Retrieve the expected filename of the resulting CSV file,
  // if given as command line parameter
  val outputCSVFile = Utilities.getOutputCSVFilePath (defaultCSVFilename, args)
  val hdfsDataFilepath = hdfsDataDir + "/" + outputCSVFile
  println ("(HDFS) File-path for the expected CSV file: " + hdfsDataFilepath)

  // Extract the SQL query from the given file
  val sqlQuery = Utilities.extractQuery (queryFile)
  println ("SQL query: " + sqlQuery)

  // Extract data from the Hive database associated with a Spark cluster
  val hiveDF = spark.sql (sqlQuery)
  hiveDF.createOrReplaceGlobalTempView (hiveTempTable)

  // Dump the resulting DataFrame into a (list of) CSV file(s)
  // The write() method on a Spark DataFrame (DF)indeed creates
  // a directory with all the chunks, which then need
  // to be re-assembled thanks to HDFS utilities (here,
  // the Utilities.merge() method)
  val tmpDir = hdfsDataDir + "/tmp"
  hiveDF.write
        .format ("com.databricks.spark.csv")
        .option ("header", "false")
        .mode ("overwrite")
        .save (tmpDir)
  Utilities.merge (tmpDir, hdfsDataFilepath)
  hiveDF.unpersist()

  // Display the first few records
  spark.sql("select * from global_temp." + hiveTempTable).show(5)

  // End of the Spark session
  spark.stop()
}

/**
  * Spark job aimed at being launched on a Hortownworks HDP 3
  * Spark cluster, typically with YARN or Mesos.
  *
  * See https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/index.html,
  * https://github.com/hortonworks-spark/spark-llap and
  * https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/integrating-hive/content/hive_hivewarehouseconnector_for_handling_apache_spark_data.html
  * for details.
  *
  *  In build.sbt, add dependencies such as:
  *  libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.2"
  *  libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.1.2.0-4"
  */
object SparkClusterHDP3QueryLauncher extends App {
  //
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("SparkClusterQueryLauncher")
    .enableHiveSupport()
    .config("hive.exec.compress.output", "false")
    .getOrCreate()

  //
  Utilities.displayVersions (spark)

  // CSV data file, from HDFS
  val hdfsDataDir = "incoming"
  val defaultCSVFilename = "hive-generic.csv"

  // File in which the SQL query is specified
  val defaultQueryFile = "requests/hive-sql-to-csv-01-test.sql"

  // Temporary Hive table name
  val hiveTempTable = "hive_generic"

  // Retrieve the name of the Hive database to connect to
  val defaultHiveDBName = "sandbox"
  val hiveDBName = Utilities.getDBName (defaultHiveDBName, args)
  println ("Hive database: '" + hiveDBName + "'")

  // Hortonworks Hive 3 specific: connect to Hive 3 with Hortonworks connector
  val hive = com.hortonworks.hwc.HiveWarehouseSession.session(spark).build()
  hive.setDatabase (hiveDBName)

  // Retrieve the filename of the SQL query, if given as command line parameter
  val queryFile = Utilities.getQueryFilePath (defaultQueryFile, args)
  println ("File-path for the SQL query: " + queryFile)

  // Retrieve the expected filename of the resulting CSV file,
  // if given as command line parameter
  val outputCSVFile = Utilities.getOutputCSVFilePath (defaultCSVFilename, args)
  val hdfsDataFilepath = hdfsDataDir + "/" + outputCSVFile
  println ("(HDFS) File-path for the expected CSV file: " + hdfsDataFilepath)

  // Extract the SQL query from the given file
  val sqlQuery = Utilities.extractQuery (queryFile)
  println ("SQL query: " + sqlQuery)

  // Extract data from the HDP Hive 3 database
  val hiveDF = hive.executeQuery (sqlQuery)
  hiveDF.createOrReplaceGlobalTempView (hiveTempTable)

  // Dump the resulting DataFrame into a (list of) CSV file(s)
  // The write() method on a Spark DataFrame (DF)indeed creates
  // a directory with all the chunks, which then need
  // to be re-assembled thanks to HDFS utilities (here,
  // the Utilities.merge() method)
  val tmpDir = hdfsDataDir + "/tmp"
  hiveDF.write
        .format ("com.databricks.spark.csv")
        .option ("header", "false")
        .mode ("overwrite")
        .save (tmpDir)
  Utilities.merge (tmpDir, hdfsDataFilepath)
  hiveDF.unpersist()

  // Display the first few records
  spark.sql("select * from global_temp." + hiveTempTable).show(5)

  // End of the Spark session
  spark.stop()
}


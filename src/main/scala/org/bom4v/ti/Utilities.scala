package org.bom4v.ti

/**
  * Utility methods
  */
object Utilities {
  /**
    * Retrieve and return the version of Scala
    */
  def getScalaVersion(): String = {
    val version:String = util.Properties.versionString
    return version
  }

  /**
    * Retrieve and return the version of Scala
    */
  def getSparkVersion (spark: org.apache.spark.sql.SparkSession): String = {
    val version:String = spark.version
    return version
  }

  /**
    * Display the versions
    */
  def displayVersions (spark: org.apache.spark.sql.SparkSession) {
    println ("Spark: " + getSparkVersion(spark))
    println ("Scala: " + getScalaVersion())
  }

  /**
    * Extract the Hive database name, potentially given as
    * command line parameter
    */
  def getDBName (
    defaultDBName: String,
    argList: Array[String]): String = {
    var dbName : String = defaultDBName
    val dbNamePattern = new scala.util.matching.Regex ("^[a-zA-Z0-9_]+$")
    for (arg <- argList) {
      val dbNameMatch = dbNamePattern.findFirstIn (arg)
      dbNameMatch.foreach { _ =>
        dbName = arg
      }
    }
    return dbName
  }

  /**
    * Extract the file-path of the SQL query, potentially given as
    * command line parameter
    */
  def getQueryFilePath (
    defaultFilePath: String,
    argList: Array[String]): String= {

    var queryFile : String = defaultFilePath
    val sqlFilePattern = new scala.util.matching.Regex ("[.]sql$")
    for (arg <- argList) {
      val sqlMatch = sqlFilePattern.findFirstIn (arg)
      sqlMatch.foreach { _ =>
        queryFile = arg
      }
    }
    return queryFile
  }

  /**
    * Extract the file-path of the expected output CSV file,
    * potentially given as command line parameter
    */
  def getOutputCSVFilePath (
    defaultFilePath:String,
    argList:Array[String]): String= {

    var csvFile : String = defaultFilePath
    val csvFilePattern = new scala.util.matching.Regex ("[.]csv(|.bz2)$")
    for (arg <- argList) {
      val csvMatch = csvFilePattern.findFirstIn (arg)
      csvMatch.foreach { _ =>
        csvFile = arg
      }
    }
    return csvFile
  }

  /**
    * Merge several file chunks into a single output file
    * https://www.oreilly.com/library/view/hadoop-the-definitive/9780596521974/ch04.html
    */
  def merge (srcPath: String, dstPath: String): Unit = {
    // The "true" setting deletes the source files once they are merged
    // into the new output
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get (hadoopConfig)
    val shouldDelete = true
    UtilityForHadoop3
      .copyMerge (hdfs, new org.apache.hadoop.fs.Path (srcPath),
        hdfs, new org.apache.hadoop.fs.Path (dstPath),
        shouldDelete, hadoopConfig)
  }

  /**
    * Sandbox to interactively debug with spark-shell
    */
  def mergeNewWay (srcPathStr: String, dstPathStr: String): Unit = {
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get (hadoopConfig)
    val srcFS = hdfs
    val dstFS = hdfs
    val dstPath = new org.apache.hadoop.fs.Path (dstPathStr)
    val outputFile = hdfs.create (dstPath)
    val srcPath = new org.apache.hadoop.fs.Path (srcPathStr)
    val factory = new org.apache.hadoop.io.compress.
      CompressionCodecFactory (hadoopConfig)
    val codec = factory.getCodec (srcPath)
    val inputStream = codec.createInputStream (hdfs.open(srcPath))
    org.apache.hadoop.io.
      IOUtils.copyBytes (inputStream, outputFile, hadoopConfig, false)
  }

  /**
    * Extract the SQL query from a file
    */
  def extractQuery (srcPath: String): String = {
    var queryString = ""
    try {

      // Get a handle on the file, so that it can be properly closed
      // when no longer used
      val bufferedSource = scala.io.Source.fromFile (srcPath)

      // Pattern/RegEx for SQL comments (anything beginning with "--")
      val commentPattern = new scala.util.matching.Regex ("[-]{2}.*$")

      // Pattern/RegEx for lines having no white space at the beginning
      // (it is an issue for SQL queries, as all the lines are just
      // concatenated, and it therefore provokes a syntax error in such cases)
      val noBegSpacePattern = new scala.util.matching.Regex ("^([^ ])")

      for (line <- bufferedSource.getLines) {
        // Get rid of SQL comments (anything beginning with "--")
        val noCommentLine = commentPattern replaceAllIn (line, "")

        // Add a white space to lines having no such white space
        // at the beginning
        val noBegSpaceLine = noBegSpacePattern replaceAllIn (noCommentLine," $1")

        //
        queryString = queryString + noBegSpaceLine
      }

      //queryString = bufferedSource.getLines.mkString
      bufferedSource.close

    } catch {
      case e: java.io.FileNotFoundException
          => println ("The '" + srcPath + "' file cannot be found")
      case e: java.io.IOException
          => println ("Got an IOException while reading the '" + srcPath
            + "' file")
    }

    return queryString
  }

}


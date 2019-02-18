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
    * Extract the file-path of the SQL query, potentially given as
    * command line parameter
    */
  def getQueryFilePath (defaultFilePath: String, args: Array[String]): String= {
    var queryFile : String = defaultFilePath
    val sqlFilePattern = new scala.util.matching.Regex ("[.]sql$")
    for (filePath <- args) {
      val sqlMatch = sqlFilePattern.findFirstIn (filePath)
      sqlMatch.foreach { _ =>
        queryFile = filePath
      }
    }
    return queryFile
  }

  /**
    * Extract the file-path of the expected output CSV file,
    * potentially given as command line parameter
    */
  def getOutputCSVFilePath (defaultFilePath:String, args:Array[String]):String= {
    var csvFile : String = defaultFilePath
    val csvFilePattern = new scala.util.matching.Regex ("[.]csv$")
    for (filePath <- args) {
      val csvMatch = csvFilePattern.findFirstIn (filePath)
      csvMatch.foreach { _ =>
        csvFile = filePath
      }
    }
    return csvFile
  }

  /**
    * Merge several file chunks into a single output file
    */
  def merge (srcPath: String, dstPath: String): Unit = {
    // The "true" setting deletes the source files once they are merged
    // into the new output
    val hadoopConfig = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get (hadoopConfig)
    org.apache.hadoop.fs.FileUtil
      .copyMerge (hdfs, new org.apache.hadoop.fs.Path (srcPath),
      hdfs, new org.apache.hadoop.fs.Path (dstPath),
      true, hadoopConfig, null)
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


// Reference: https://stackoverflow.com/a/50545815/798053

package org.bom4v.ti

import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException

/**
  * Utilities
  */
object UtilityForHadoop3 {

  /**
    * Re-implementation of the Hadoop CopyMerge() method,
    * which has been removed in Hadoop 3
    */
  def copyMergeOldWay (srcFS: FileSystem, srcDir: Path,
    dstFS: FileSystem, dstFile: Path,
    deleteSource: Boolean, conf: Configuration): Boolean = {

    if (dstFS.exists(dstFile)) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)
      scala.util.Try {
        srcFS
          .listStatus (srcDir)
          .sortBy (_.getPath.getName)
          .collect {
          case status if status.isFile() =>
            val inputFile = srcFS.open (status.getPath())
            scala.util.Try (IOUtils.copyBytes (inputFile, outputFile,
              conf, false))
            inputFile.close()
        }
      }
      outputFile.close()

      if (deleteSource) {
        srcFS.delete (srcDir, true)

      } else true

    } else false

  }

  /**
    * Re-implementation of the Hadoop CopyMerge() method,
    * which has been removed in Hadoop 3.
    * https://www.oreilly.com/library/view/hadoop-the-definitive/9780596521974/ch04.html
    * That implementation checks for the compression codec,
    * and decompresses the input files based on that codec.
    * The output file is compressed with BZip2
    */
  def copyMerge (
    srcFS: org.apache.hadoop.fs.FileSystem,
    srcDir: org.apache.hadoop.fs.Path,
    dstFS: org.apache.hadoop.fs.FileSystem,
    dstFile: org.apache.hadoop.fs.Path,
    deleteSource: Boolean,
    hadoopConfig: org.apache.hadoop.conf.Configuration
  ): Boolean =  {
    if (dstFS.exists (dstFile)) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val factory = new org.apache.hadoop.io.compress.
        CompressionCodecFactory (hadoopConfig)
      val bzCodec = new org.apache.hadoop.io.compress.BZip2Codec()
      bzCodec.setConf (hadoopConfig)

      val outputFile = dstFS.create (dstFile)
      val outputStream = bzCodec.createOutputStream (outputFile)
      val srcDirContent = srcFS.listStatus (srcDir).sortBy (_.getPath.getName)

      scala.util.Try {
        srcDirContent.collect {
          case status if status.isFile() =>
            val srcPath = status.getPath()
            val srcPathStr = srcPath.getName
            val csvFilePattern = new scala.util.matching.Regex ("part-.*[.]csv(|[.]deflate)$")
            val csvMatch = csvFilePattern.findFirstIn (srcPathStr)
            csvMatch.foreach { _ =>
              // println ("Will uncompress " + srcPathStr)
              val codec = factory.getCodec (srcPath)
              val inputFile = srcFS.open (srcPath)
              val inputStream = codec.createInputStream (inputFile)
              scala.util.Try {
                org.apache.hadoop.io.
                  IOUtils.copyBytes (inputStream, outputStream, hadoopConfig, false)
              }
              inputStream.close()
            }
        }
      }
      outputStream.close()

      if (deleteSource) {
        srcFS.delete (srcDir, true)

      } else true

    } else false

  }

}


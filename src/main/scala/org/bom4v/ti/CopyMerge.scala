// Reference: https://stackoverflow.com/a/50545815/798053
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
  def copyMerge (srcFS: FileSystem, srcDir: Path,
    dstFS: FileSystem, dstFile: Path,
    deleteSource: Boolean, conf: Configuration): Boolean = {

    if (dstFS.exists(dstFile)) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus (srcDir)
          .sortBy (_.getPath.getName)
          .collect {
          case status if status.isFile() =>
            val inputFile = srcFS.open (status.getPath())
            Try (IOUtils.copyBytes(inputFile, outputFile, conf, false))
            inputFile.close()
        }
      }
      outputFile.close()

      if (deleteSource) {
        srcFS.delete (srcDir, true)

      } else true

    } else false

  }

}


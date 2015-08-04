package com.intenthq.pucket.util

import org.apache.hadoop.fs.{Path, FileSystem, FileStatus}

import scalaz.\/

/** Utility object for working with HDFS */
object HadoopUtil {
  /** Recursively list files under a path
    *
    * @param path path to be listed
    * @param fs hadoop filesytem instance
    * @param extension file extension to filter by
    * @return a sequence of paths
    */
  def listFiles(path: Path, fs: FileSystem, extension: String): Throwable \/ Seq[Path] =
    fileStatuses(path, fs, extension).map(_.map(_.getPath))

  /** Recursively list file statuses under a path
    *
    * @param path path to be listed
    * @param fs hadoop filesytem instance
    * @param extension file extension to filter by
    * @return a sequence of file statuses
    */
  def fileStatuses(path: Path, fs: FileSystem, extension: String): Throwable \/ Seq[FileStatus] =
    \/.fromTryCatchNonFatal(recursiveFileStatus(fs.listStatus(path), fs).filter(_.getPath.getName.endsWith(extension)))

  private def recursiveFileStatus(files: Seq[FileStatus], fs: FileSystem): Seq[FileStatus] =
    files.flatMap( file =>
      if (file.isDirectory) recursiveFileStatus(fs.listStatus(file.getPath), fs)
      else Seq(file)
    )
}

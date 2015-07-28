package com.intenthq.pucket

import java.io.File

import com.google.common.io.Files
import com.intenthq.pucket.writer.Writer
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.specs2.matcher.MatchResult

import scalaz.\/
import scalaz.syntax.either._

object TestUtils {


  def writeData[T, Ex](data: Seq[T], writer: Writer[T, Ex]): Ex \/ Writer[T, Ex] =
    data.foldLeft(writer.right[Ex])( (acc, d) =>
                                              acc.fold(_.left, _.write(d))
    )

  case class PucketWrapper[T](dir: File, path: Path, pucket: Throwable \/ Pucket[T]) {
    def close(): Unit = FileUtils.deleteDirectory(dir)

    def runTest(test: Throwable \/ Pucket[T] => MatchResult[Throwable \/ Pucket[T]]): MatchResult[Throwable \/ Pucket[T]] =
      runTest(pucket, test)

    def runTest(p: Throwable \/ Pucket[T],
                test: Throwable \/ Pucket[T] => MatchResult[Throwable \/ Pucket[T]]): MatchResult[Throwable \/ Pucket[T]] = {
      val result = test(p)
      close()
      result
    }
  }

  val fs: FileSystem = FileSystem.get(new Configuration())
  def path(dir: File) = fs.makeQualified(new Path(dir.getAbsolutePath + "/data/pucket"))
  def mkdir: File = Files.createTempDir()
}

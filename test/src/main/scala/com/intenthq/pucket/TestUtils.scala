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

  def readData[T](data: Seq[T], pucket: Pucket[T]): Throwable \/ Seq[T] =
    pucket.reader.flatMap(r =>
      data.indices.foldLeft((Seq[T](), r).right[Throwable])( (acc, v) =>
        acc.fold(ex => return ex.left, x => x._2.read.fold(ex => return ex.left, y =>
          (y._1.fold(return x._2.close.map(_ => x._1))(x._1 ++ Seq(_)), y._2).right[Throwable]))
      )
    ).flatMap(x => x._2.close.map(_ => x._1))


  val fs: FileSystem = FileSystem.get(new Configuration())
  def path(dir: File) = fs.makeQualified(new Path(dir.getAbsolutePath + "/data/pucket"))
  def mkdir: File = Files.createTempDir()
}

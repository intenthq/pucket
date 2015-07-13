package com.intenthq.pucket.writer

import com.intenthq.pucket.TestUtils._
import com.intenthq.test.model.Test
import org.specs2.Specification
import org.specs2.matcher.DisjunctionMatchers

import scalaz.\/
import scalaz.syntax.either._

class IncremantalWriterSpec extends Specification with DisjunctionMatchers {
  import IncremantalWriterSpec._
  def is =
    s2"""
        Writes the expected number of files $writeTest
        Removes the temp dir ${step(pucketWrapper.close())}
      """

  def writeAndVerify = writeTest and verifyFiles

  def writeTest =
    writeData must be_\/-.like {
      case a => a.close must be_\/-
    }

  def verifyFiles =
    numberOfFiles must be_\/-.like {
      case a => a === expectedFiles
    }
}

object IncremantalWriterSpec {
  val rng = scala.util.Random
  val data = 1.to(500).map(_ => new Test(rng.nextLong()))
  val maxSize = 50

  val expectedFiles = data.length / maxSize

  val pucketWrapper = PucketWrapper.apply()
  val writer = pucketWrapper.pucket.fold(th => (0L, th).left, p => IncrementalWriter[Test](0, p, maxSize))


  def writeData: (Long, Throwable) \/ IncrementalWriter[Test] =
    data.foldLeft(writer)( (acc, d) =>
      acc.fold(_.left, w => w.write(d, w.checkPoint + 1))
    )

  def numberOfFiles: Throwable \/ Int =
    pucketWrapper.pucket.flatMap(p => pucketWrapper.pucket.map(x => p.fs.listStatus(x.path)
      .count(_.getPath.getName.contains(".parquet"))))
}



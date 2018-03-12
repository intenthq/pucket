package com.intenthq.pucket.spark

import com.intenthq.pucket.{Pucket, TestLogging}
import com.intenthq.pucket.spark.PucketSparkAdapter._
import org.apache.commons.io.FileUtils
import org.specs2.Specification
import org.specs2.scalaz.DisjunctionMatchers

import scala.reflect.ClassTag
import scalaz.\/

abstract class PucketSparkAdapterSpec[T, Descriptor](override val registrator: Option[String] = None)
                                                    (implicit ct: ClassTag[T]) extends Specification
                                                                               with LocalSparkSpec
                                                                               with DisjunctionMatchers
                                                                               with TestLogging {
  import PucketSparkAdapterSpec._
  import com.intenthq.pucket.TestUtils._

  def descriptor: Descriptor
  def newData(i: Long): T
  val pucket: Throwable \/ Pucket[T]
  val data: Seq[T] = 0.to(10).map(_ => newData(rng.nextLong()))

  def findPucket: String => Throwable \/ Pucket[T]

  def is = test

  def test =
    s2"""
        Writes data to test directory ${step(pucket.flatMap(_.writer).flatMap(writeData(data, _)).flatMap(_.close))}
        The RDD contains the expected data $sameData
        The RDD can be written out to the output dir $writeOut
        Cleans up test directory ${step(FileUtils.deleteDirectory(dir))}
      """

  val dir = mkdir

  val outputPath = dir.getAbsolutePath + "/output"

  def sameData = pucket.map(_.toRDD.collect().toSeq) must beRightDisjunction.like {
    case a => a must containAllOf(data)
  }

  def writeOut = pucket.map(x => x.toRDD.saveAsPucket(outputPath, x.descriptor, Some(x.conf))) must beRightDisjunction.like {
    case _ => findPucket(outputPath) must beRightDisjunction.like {
      case p => readData(data, p) must beRightDisjunction.like {
        case a => a must containAllOf(data)
      }
    }
  }
}

object PucketSparkAdapterSpec {
  val rng = scala.util.Random
}

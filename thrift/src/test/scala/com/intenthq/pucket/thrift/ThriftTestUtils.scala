package com.intenthq.pucket.thrift

import java.io.File

import com.intenthq.pucket.{Pucket, TestUtils}
import com.intenthq.pucket.TestUtils._
import com.intenthq.pucket.util.Partitioner
import com.intenthq.pucket.test.model.ThriftTest
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalacheck.Gen

import scalaz.\/
import scalaz.syntax.either._

object ThriftTestUtils {
  val descriptor: ThriftPucketDescriptor[ThriftTest] =
    ThriftPucketDescriptor(classOf[ThriftTest], CompressionCodecName.SNAPPY, Some(ModPartitioner))

  def createWrapper(dir: File): PucketWrapper[ThriftTest] =
    PucketWrapper(dir, path(dir), ThriftPucket.create(path(dir), fs, descriptor))

  def createWrapper: PucketWrapper[ThriftTest] = {
    val dir = mkdir
    createWrapper(dir)
  }

  def descriptorGen: Gen[ThriftPucketDescriptor[ThriftTest]] = for {
    compression <- Gen.oneOf(CompressionCodecName.values())
    partitioner <- Gen.oneOf(List(Some(ModPartitioner), Some(PassThroughPartitioner), None))
  } yield ThriftPucketDescriptor[ThriftTest](classOf[ThriftTest], compression, partitioner)


  object ModPartitioner extends Partitioner[ThriftTest] {
    override def partition(data: ThriftTest, pucket: Pucket[ThriftTest]): Throwable \/ Pucket[ThriftTest] =
      pucket.subPucket(new Path((data.getTest % 20).toString))
  }

  object PassThroughPartitioner extends Partitioner[ThriftTest] {
    override def partition(data: ThriftTest, pucket: Pucket[ThriftTest]): \/[Throwable, Pucket[ThriftTest]] = pucket.right
  }

}

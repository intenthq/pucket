package com.intenthq.thrift

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.thrift.ThriftPucketDescriptor
import com.intenthq.pucket.util.Partitioner
import com.intenthq.test.model.Test
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalacheck.Gen

import scalaz.\/
import scalaz.syntax.either._

object ThriftTestUtils {

  def descriptorGen: Gen[ThriftPucketDescriptor[Test]] = for {
    compression <- Gen.oneOf(CompressionCodecName.values())
    partitioner <- Gen.oneOf(List(Some(ModPartitioner), Some(PassThroughPartitioner), None))
  } yield ThriftPucketDescriptor[Test](classOf[Test], compression, partitioner)


  object ModPartitioner extends Partitioner[Test] {
    override def partition(data: Test, pucket: Pucket[Test]): Throwable \/ Pucket[Test] =
      pucket.subPucket(new Path((data.getTest % 20).toString))
  }

  object PassThroughPartitioner extends Partitioner[Test] {
    override def partition(data: Test, pucket: Pucket[Test]): \/[Throwable, Pucket[Test]] = pucket.right
  }

}

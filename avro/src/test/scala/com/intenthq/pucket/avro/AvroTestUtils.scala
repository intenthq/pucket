package com.intenthq.pucket.avro

import java.io.File

import com.intenthq.pucket.TestUtils
import com.intenthq.pucket.TestUtils.{PucketWrapper, _}
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.util.PucketPartitioner
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalacheck.Gen

object AvroTestUtils {
  val descriptor = AvroPucketDescriptor[AvroTest](AvroTest.getClassSchema, CompressionCodecName.SNAPPY)

  def createWrapper(dir: File): PucketWrapper[AvroTest] =
    PucketWrapper(dir, path(dir), AvroPucket.create(path(dir), fs, descriptor))

  def createWrapper: PucketWrapper[AvroTest] = {
    val dir = mkdir
    createWrapper(dir)
  }

  def descriptorGen: Gen[AvroPucketDescriptor[AvroTest]] = for {
    compression <- Gen.oneOf(CompressionCodecName.SNAPPY, CompressionCodecName.UNCOMPRESSED)
    partitioner <- Gen.oneOf(Some(ModPucketPartitioner), Some(PassThroughPucketPartitioner), None)
  } yield AvroPucketDescriptor(AvroTest.getClassSchema, compression, partitioner)

  object ModPucketPartitioner extends PucketPartitioner[AvroTest] {
    override def partition(data: AvroTest): Path =
      new Path((data.getTest % 20).toString)
  }

  object PassThroughPucketPartitioner extends PucketPartitioner[AvroTest] {
    override def partition(data: AvroTest): Path = new Path(".")
  }
}
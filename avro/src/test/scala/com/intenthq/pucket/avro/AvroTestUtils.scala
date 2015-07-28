package com.intenthq.pucket.avro

import java.io.File

import com.intenthq.pucket.{TestUtils, Pucket}
import TestUtils._
import com.intenthq.pucket.TestUtils.PucketWrapper
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.util.Partitioner
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/
import scalaz.syntax.either._

object AvroTestUtils {
  val descriptor = AvroPucketDescriptor(AvroTest.getClassSchema, CompressionCodecName.SNAPPY, Some(ModPartitioner))

  def createWrapper(dir: File): PucketWrapper[AvroTest] =
    PucketWrapper(dir, path(dir), AvroPucket.create(path(dir), fs, descriptor))

  def createWrapper: PucketWrapper[AvroTest] = {
    val dir = mkdir
    createWrapper(dir)
  }

  object ModPartitioner extends Partitioner[AvroTest] {
    override def partition(data: AvroTest, pucket: Pucket[AvroTest]): Throwable \/ Pucket[AvroTest] =
      pucket.subPucket(new Path((data.getTest % 20).toString))
  }

  object PassThroughPartitioner extends Partitioner[AvroTest] {
    override def partition(data: AvroTest, pucket: Pucket[AvroTest]): \/[Throwable, Pucket[AvroTest]] = pucket.right
  }
}

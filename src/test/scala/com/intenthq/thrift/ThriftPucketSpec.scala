package com.intenthq.thrift

import java.io.File

import com.intenthq.PhucketTestSuite
import com.intenthq.pucket.thrift.{ThriftPucket, ThriftPucketDescriptor}
import com.intenthq.pucket.{Pucket, PucketDescriptor}
import com.intenthq.test.model.Test
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalacheck.Gen

import scalaz.\/

class ThriftPucketSpec extends PhucketTestSuite[Test]  {
  import PhucketTestSuite._
  import ThriftTestUtils._

  override def findExisting(path: Path): \/[Throwable, Pucket[Test]] = ThriftPucket(path, fs, classOf[Test])

  override val descriptor: ThriftPucketDescriptor[Test] =
    ThriftPucketDescriptor(classOf[Test], CompressionCodecName.SNAPPY, Some(ModPartitioner))

  override def createWrapper(dir: File): PucketWrapper[Test] =
    PucketWrapper(dir, path(dir), ThriftPucket.create(path(dir), fs, descriptor))

  override def createWrapper(descriptor: PucketDescriptor[Test]): PucketWrapper[Test] = {
    val dir = mkdir
    PucketWrapper(dir, path(dir), ThriftPucket.create(path(dir), fs, descriptor.asInstanceOf[ThriftPucketDescriptor[Test]]))
  }

  override def createWrapper: PucketWrapper[Test] = {
    val dir = mkdir
    createWrapper(dir)
  }

  override def findOrCreate(path: Path, descriptor: PucketDescriptor[Test]): \/[Throwable, Pucket[Test]] =
    ThriftPucket.findOrCreate(path, fs, descriptor.asInstanceOf[ThriftPucketDescriptor[Test]])

  override val data: Seq[Test] = 0.to(10).map(_ => new Test(rng.nextLong()))

  override def findOrCreatePW: PucketWrapper[Test] = {
    val dir = mkdir
    PucketWrapper(dir, path(dir), findOrCreate(path(dir), descriptor))
  }

  override def descriptorGen: Gen[ThriftPucketDescriptor[Test]] = ThriftTestUtils.descriptorGen

}



package com.intenthq.pucket.thrift

import com.intenthq.pucket.TestUtils._
import com.intenthq.pucket.{Pucket, PucketSpec, TestUtils}
import com.intenthq.pucket.test.model.ThriftTest
import org.apache.hadoop.fs.Path
import org.scalacheck.Gen

import scalaz.\/


class ThriftPucketSpec extends PucketSpec[ThriftTest, ThriftPucketDescriptor[ThriftTest]] {

  override def descriptor: ThriftPucketDescriptor[ThriftTest] = ThriftTestUtils.descriptor

  override def descriptorGen: Gen[ThriftPucketDescriptor[ThriftTest]] = ThriftTestUtils.descriptorGen

  override def newData(i: Long): ThriftTest = new ThriftTest(i)

  override def createPucket(path: Path, descriptor: ThriftPucketDescriptor[ThriftTest]): \/[Throwable, Pucket[ThriftTest]] =
    ThriftPucket.create(path, fs, descriptor)

  override def findOrCreate(path: Path, descriptor: ThriftPucketDescriptor[ThriftTest]): \/[Throwable, Pucket[ThriftTest]] =
    ThriftPucket.findOrCreate(path, fs, descriptor)

  override def findPucket(path: Path): Throwable \/ Pucket[ThriftTest] = ThriftPucket(path, fs, classOf[ThriftTest])
}



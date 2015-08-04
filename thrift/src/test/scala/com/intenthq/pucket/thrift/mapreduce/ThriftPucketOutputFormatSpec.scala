package com.intenthq.pucket.thrift.mapreduce

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.mapreduce.PucketOutputFormatSpec
import com.intenthq.pucket.test.model.ThriftTest
import com.intenthq.pucket.TestUtils._
import com.intenthq.pucket.thrift.ThriftTestUtils._
import com.intenthq.pucket.thrift.{ThriftTestUtils, ThriftPucket, ThriftPucketDescriptor}
import org.apache.hadoop.fs.Path
import org.scalacheck.Gen

import scalaz.\/

class ThriftPucketOutputFormatSpec extends PucketOutputFormatSpec[ThriftTest, ThriftPucketDescriptor[ThriftTest]] {

  override val pucket: \/[Throwable, Pucket[ThriftTest]] = ThriftPucket.create(path(dir), fs, descriptor)

  override def readSupport: \/[Throwable, Class[_]] = pucket.map(_.descriptor.readSupportClass)

  override def findPucket(path: Path): \/[Throwable, Pucket[ThriftTest]] = ThriftPucket(path, fs, classOf[ThriftTest])

  override def descriptorGen: Gen[ThriftPucketDescriptor[ThriftTest]] = ThriftTestUtils.descriptorGen

  override def newData(i: Long): ThriftTest = new ThriftTest(i)

  override def writeClass: Class[ThriftTest] = classOf[ThriftTest]
}

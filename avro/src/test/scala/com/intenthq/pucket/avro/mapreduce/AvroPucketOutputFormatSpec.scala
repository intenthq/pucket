package com.intenthq.pucket.avro.mapreduce

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.TestUtils._
import com.intenthq.pucket.avro.AvroTestUtils._
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.avro.{AvroPucket, AvroPucketDescriptor, AvroTestUtils}
import com.intenthq.pucket.mapreduce.PucketOutputFormatSpec
import org.apache.hadoop.fs.Path
import org.scalacheck.Gen

import scalaz.\/

class AvroPucketOutputFormatSpec extends PucketOutputFormatSpec[AvroTest, AvroPucketDescriptor[AvroTest]] {

  override val pucket: \/[Throwable, Pucket[AvroTest]] =
    AvroPucket.create(path(dir), fs, descriptor)

  override def findPucket(path: Path): \/[Throwable, Pucket[AvroTest]] =
    AvroPucket[AvroTest](path, fs, AvroTest.getClassSchema)

  override def descriptorGen: Gen[AvroPucketDescriptor[AvroTest]] = AvroTestUtils.descriptorGen

  override def newData(i: Long): AvroTest = new AvroTest(i)

  override def readSupport: Throwable \/ Class[_] = pucket.map(_.descriptor.readSupportClass)

  override def writeClass: Class[AvroTest] = classOf[AvroTest]
}

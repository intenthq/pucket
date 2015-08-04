package com.intenthq.pucket.avro

import com.intenthq.pucket.TestUtils._
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.{Pucket, PucketSpec}
import org.apache.hadoop.fs.Path
import org.scalacheck.Gen

import scalaz.\/

class AvroPucketSpec extends PucketSpec[AvroTest, AvroPucketDescriptor[AvroTest]]{


  override def newData(i: Long): AvroTest = new AvroTest(i)

  override def descriptor: AvroPucketDescriptor[AvroTest] = AvroTestUtils.descriptor

  override def findOrCreate(path: Path,
                            descriptor: AvroPucketDescriptor[AvroTest]): \/[Throwable, Pucket[AvroTest]] =
    AvroPucket.findOrCreate(path, fs, descriptor)

  override def findPucket(path: Path): \/[Throwable, Pucket[AvroTest]] =
    AvroPucket(path, fs, AvroTest.getClassSchema)

  override def createPucket(path: Path,
                            descriptor: AvroPucketDescriptor[AvroTest]): \/[Throwable, Pucket[AvroTest]] =
    AvroPucket.create(path, fs, descriptor)

  override def descriptorGen: Gen[AvroPucketDescriptor[AvroTest]] = AvroTestUtils.descriptorGen
}

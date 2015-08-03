package com.intenthq.pucket.avro.spark

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.avro.{AvroPucket, AvroPucketDescriptor}
import com.intenthq.pucket.spark.PucketSparkAdapterSpec
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/

class AvroPucketSparkAdapterSpec extends PucketSparkAdapterSpec[AvroTest, AvroPucketDescriptor[AvroTest]](Some(classOf[AvroRegistrator].getName)) {
  import com.intenthq.pucket.TestUtils._

  override val descriptor: AvroPucketDescriptor[AvroTest] =
    AvroPucketDescriptor(AvroTest.getClassSchema, CompressionCodecName.SNAPPY)

  override val pucket: Throwable \/ Pucket[AvroTest] = AvroPucket.create(path(dir), fs, descriptor)

  override def newData(i: Long): AvroTest = new AvroTest(i)

  override def findPucket: (String) => \/[Throwable, Pucket[AvroTest]] = path =>
    AvroPucket(new Path(path), fs, AvroTest.getClassSchema)
}

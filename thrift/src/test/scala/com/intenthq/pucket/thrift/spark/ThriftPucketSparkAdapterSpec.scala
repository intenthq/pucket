package com.intenthq.pucket.thrift.spark

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.spark.PucketSparkAdapterSpec
import com.intenthq.pucket.test.model.ThriftTest
import com.intenthq.pucket.thrift.{ThriftPucket, ThriftPucketDescriptor}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/

class ThriftPucketSparkAdapterSpec extends PucketSparkAdapterSpec[ThriftTest, ThriftPucketDescriptor[ThriftTest]] {
  import com.intenthq.pucket.TestUtils._

  override def descriptor: ThriftPucketDescriptor[ThriftTest] =
    ThriftPucketDescriptor(classOf[ThriftTest], CompressionCodecName.SNAPPY)

  override def findPucket: (String) => \/[Throwable, Pucket[ThriftTest]] = path =>
    ThriftPucket(new Path(path), fs, classOf[ThriftTest])

  override def newData(i: Long): ThriftTest = new ThriftTest(i)

  override val pucket: \/[Throwable, Pucket[ThriftTest]] = ThriftPucket.create(path(dir), fs, descriptor)
}

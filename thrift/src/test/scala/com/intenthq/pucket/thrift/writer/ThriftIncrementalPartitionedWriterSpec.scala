package com.intenthq.pucket.thrift.writer

import com.intenthq.pucket.test.model.ThriftTest
import com.intenthq.pucket.thrift.ThriftTestUtils.ModPucketPartitioner
import com.intenthq.pucket.thrift.{ThriftPucket, ThriftPucketDescriptor}
import com.intenthq.pucket.writer.IncrementalPartitionedWriterSpec
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class ThriftIncrementalPartitionedWriterSpec extends IncrementalPartitionedWriterSpec[ThriftTest] {
  import com.intenthq.pucket.TestUtils._

  override val wrapper: PucketWrapper[ThriftTest] = {
    val dir = mkdir
    PucketWrapper(dir, path(dir), ThriftPucket.create(path(dir), fs, ThriftPucketDescriptor(classOf[ThriftTest],
                                                                                        CompressionCodecName.SNAPPY,
                                                                                        Some(ModPucketPartitioner))))
  }

  override def newData(i: Long): ThriftTest = new ThriftTest(i)
}
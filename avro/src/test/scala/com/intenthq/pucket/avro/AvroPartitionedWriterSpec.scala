package com.intenthq.pucket.avro

import com.intenthq.pucket.avro.AvroTestUtils.ModPucketPartitioner
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.writer.PartitionedWriterSpec
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class AvroPartitionedWriterSpec extends PartitionedWriterSpec[AvroTest] {
  import com.intenthq.pucket.TestUtils._

  override val wrapper: PucketWrapper[AvroTest] = {
    val dir = mkdir
    PucketWrapper(dir, path(dir), AvroPucket.create(path(dir), fs, AvroPucketDescriptor(AvroTest.getClassSchema,
                                                                                        CompressionCodecName.SNAPPY,
                                                                                        Some(ModPucketPartitioner))))
  }
  override def newData(i: Long):AvroTest  = new AvroTest(i)
}

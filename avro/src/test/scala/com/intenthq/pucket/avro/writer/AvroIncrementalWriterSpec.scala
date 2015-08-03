package com.intenthq.pucket.avro.writer

import com.intenthq.pucket.TestUtils.PucketWrapper
import com.intenthq.pucket.avro.AvroTestUtils
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.writer.IncrementalWriterSpec

class AvroIncrementalWriterSpec extends IncrementalWriterSpec[AvroTest] {

  override val wrapper: PucketWrapper[AvroTest] = AvroTestUtils.createWrapper

  override def newData(i: Long): AvroTest = new AvroTest(i)
}

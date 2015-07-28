package com.intenthq.pucket.thrift

import com.intenthq.pucket.TestUtils
import TestUtils.PucketWrapper
import com.intenthq.pucket.writer.PartitionedWriterSpec
import com.intenthq.pucket.test.model.ThriftTest

class ThriftPartitionedWriterSpec extends PartitionedWriterSpec[ThriftTest] {

  override val wrapper: PucketWrapper[ThriftTest] = ThriftTestUtils.createWrapper

  override def newData(i: Long): ThriftTest = new ThriftTest(i)
}

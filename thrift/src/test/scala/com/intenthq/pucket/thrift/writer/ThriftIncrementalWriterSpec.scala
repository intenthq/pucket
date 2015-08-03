package com.intenthq.pucket.thrift.writer

import com.intenthq.pucket.TestUtils
import com.intenthq.pucket.TestUtils.PucketWrapper
import com.intenthq.pucket.test.model.ThriftTest
import com.intenthq.pucket.thrift.ThriftTestUtils
import com.intenthq.pucket.writer.IncrementalWriterSpec

class ThriftIncrementalWriterSpec extends IncrementalWriterSpec[ThriftTest] {

  override val wrapper: PucketWrapper[ThriftTest] = ThriftTestUtils.createWrapper

  override def newData(i: Long): ThriftTest = new ThriftTest(i)
}

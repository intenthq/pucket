package com.intenthq.pucket.avro.spark

import com.esotericsoftware.kryo.Kryo
import com.intenthq.pucket.avro.test.AvroTest
import com.twitter.chill.avro.AvroSerializer
import org.apache.spark.serializer.KryoRegistrator

class AvroRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit =
    kryo.register(classOf[AvroTest], AvroSerializer.SpecificRecordBinarySerializer[AvroTest])

}

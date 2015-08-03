package com.intenthq.pucket.avro

import com.intenthq.pucket.javacompat.avro.AvroPucketInstantiator
import com.intenthq.pucket.util.PucketPartitioner
import com.intenthq.pucket.{PucketDescriptorCompanion, Pucket, PucketDescriptor}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, JValue, _}

import scalaz.\/
import scalaz.syntax.either._

case class AvroPucketDescriptor[T <: IndexedRecord](schema: Schema,
                                                    override val compression: CompressionCodecName,
                                                    override val partitioner: Option[PucketPartitioner[T]] = None) extends PucketDescriptor[T] {
  import AvroPucketDescriptor._

  implicit val formats = DefaultFormats

  override def instantiatorClass: Class[_] = classOf[AvroPucketInstantiator]

  override def json: JValue = JField(avroSchemaKey, schema.toString) ~ commonJson
}

object AvroPucketDescriptor extends PucketDescriptorCompanion {
  import PucketDescriptor._

  type HigherType = IndexedRecord
  type V = Schema

  val avroSchemaKey = "avroSchema"

  private def validate[T <: HigherType](descriptorString: String): Throwable \/ (JValue, Schema, CompressionCodecName, Option[PucketPartitioner[T]]) =
    for {
      underlying <- parseDescriptor[T](descriptorString)
      schema <- extractValue(underlying._1, avroSchemaKey)
      parsedSchema <- \/.fromTryCatchNonFatal(parse(schema))
      avroSchema <- \/.fromTryCatchNonFatal(new Schema.Parser().parse(schema))
    } yield (parsedSchema, avroSchema, underlying._2, underlying._3)

  override def apply[T <: IndexedRecord](expectedSchema: Schema, descriptorString: String): Throwable \/ AvroPucketDescriptor[T] =
    for {
      underlying <- validate[T](descriptorString)
      expectedSchemaJson <- \/.fromTryCatchNonFatal(parse(expectedSchema.toString))
      _ <- if (underlying._2 == expectedSchema) ().right
           else new RuntimeException("Found schema does not match excpected").left
    } yield AvroPucketDescriptor[T](underlying._2, underlying._3, underlying._4)

  override def apply[T <: HigherType](descriptorString: String): Throwable \/ AvroPucketDescriptor[T] =
    validate[T](descriptorString).map(underlying => AvroPucketDescriptor[T](underlying._2, underlying._3, underlying._4))
}



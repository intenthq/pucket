package com.intenthq.pucket.avro

import com.intenthq.pucket.util.Partitioner
import com.intenthq.pucket.{Pucket, PucketDescriptor}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, JValue, _}

import scalaz.\/

case class AvroPucketDescriptor[T <: IndexedRecord](schema: Schema,
                                                    override val compression: CompressionCodecName,
                                                    override val partitioner: Option[Partitioner[T]] = None) extends PucketDescriptor[T] {
  import AvroPucketDescriptor._

  implicit val formats = DefaultFormats

  override def json: JValue = JField(avroSchemaKey, parse(schema.toString)) ~ commonJson
}

object AvroPucketDescriptor {
  import PucketDescriptor._

  val avroSchemaKey = "avroSchema"

  def apply[T <: IndexedRecord](expectedSchema: Schema, descriptorString: String): Throwable \/ AvroPucketDescriptor[T] =
    for {
      underlying <- parseDescriptor[T](descriptorString)
      schema <- extractValue(underlying._1, avroSchemaKey)
      expectedSchemaJson <- \/.fromTryCatchNonFatal(parse(expectedSchema.toString))
      _ <- Pucket.compareDescriptors(parse(schema.toString), expectedSchemaJson)
      avroSchema <- \/.fromTryCatchNonFatal(new Schema.Parser().parse(schema))
    } yield new AvroPucketDescriptor[T](avroSchema, underlying._2, underlying._3)
}


package com.intenthq.pucket.avro

import com.intenthq.pucket.avro.mapreduce.AvroPucketInstantiator
import com.intenthq.pucket.util.PucketPartitioner
import com.intenthq.pucket.{PucketDescriptor, PucketDescriptorCompanion}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/
import scalaz.syntax.either._

/** Avro pucket descriptor implementation
  *
  * @param schema Avro schema instance
  * @param compression Parquet compression codec
  * @param partitioner Optional pucket partitioner implementation
  * @tparam T the type of data being stored in the pucket
  */
case class AvroPucketDescriptor[T <: IndexedRecord](schema: Schema,
                                                    override val compression: CompressionCodecName,
                                                    override val partitioner: Option[PucketPartitioner[T]] = None) extends PucketDescriptor[T] {
  import AvroPucketDescriptor._

  /** @inheritdoc */
  override def instantiatorClass: Class[_] = classOf[AvroPucketInstantiator]

  /** @inheritdoc */
  override def json: Map[String, String] = Map(avroSchemaKey -> schema.toString) ++ commonJson

  /** @inheritdoc */
  override val readSupportClass: Class[AvroReadSupport[T]] = classOf[AvroReadSupport[T]]
}

/** Factory object for [[com.intenthq.pucket.avro.AvroPucketDescriptor]]] */
object AvroPucketDescriptor extends PucketDescriptorCompanion {
  import PucketDescriptor._

  type HigherType = IndexedRecord
  type S = Schema

  val avroSchemaKey = "avroSchema"

  private def validate[T <: HigherType](descriptorString: String): Throwable \/ (Map[String, String], Schema, CompressionCodecName, Option[PucketPartitioner[T]]) =
    for {
      underlying <- parseDescriptor[T](descriptorString)
      schema <- extractValue(underlying._1, avroSchemaKey)
      parsedSchema <- parse(schema)
      avroSchema <- \/.fromTryCatchNonFatal(new Schema.Parser().parse(schema))
    } yield (parsedSchema, avroSchema, underlying._2, underlying._3)

  /** @inheritdoc */
  override def apply[T <: IndexedRecord](expectedSchema: Schema, descriptorString: String): Throwable \/ AvroPucketDescriptor[T] =
    for {
      underlying <- validate[T](descriptorString)
      expectedSchemaJson <- parse(expectedSchema.toString)
      _ <- if (underlying._2 == expectedSchema) ().right
           else new RuntimeException("Found schema does not match expected").left
    } yield AvroPucketDescriptor[T](underlying._2, underlying._3, underlying._4)

  /** @inheritdoc */
  override def apply[T <: HigherType](descriptorString: String): Throwable \/ AvroPucketDescriptor[T] =
    validate[T](descriptorString).map(underlying => AvroPucketDescriptor[T](underlying._2, underlying._3, underlying._4))
}



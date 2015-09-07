package com.intenthq.pucket.thrift

import com.intenthq.pucket.thrift.mapreduce.ThriftPucketInstantiator
import com.intenthq.pucket.util.PucketPartitioner
import com.intenthq.pucket.{PucketDescriptor, PucketDescriptorCompanion}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.thrift.ThriftReadSupport

import scalaz.\/
import scalaz.syntax.either._

/** Thrift pucket descriptor implementation
 *
 * @param schemaClass Thrift schema class
 * @param compression Parquet compression codec
 * @param partitioner Optional pucket partitioner implementation
 * @tparam T the type of data being stored in the pucket
 */
case class ThriftPucketDescriptor[T <: Thrift](schemaClass: Class[T],
                                               override val compression: CompressionCodecName,
                                               override val partitioner: Option[PucketPartitioner[T]] = None) extends PucketDescriptor[T] {
  import ThriftPucketDescriptor._

  /** @inheritdoc  */
  override val json: Map[String, String] = Map(schemaClassKey -> schemaClass.getName) ++ commonJson

  /** @inheritdoc */
  override def instantiatorClass: Class[_] = classOf[ThriftPucketInstantiator]

  /** @inheritdoc */
  override val readSupportClass: Class[ThriftReadSupport[T]] = classOf[ThriftReadSupport[T]]
}

/** Factory object for [[com.intenthq.pucket.thrift.ThriftPucketDescriptor]]] */
object  ThriftPucketDescriptor extends PucketDescriptorCompanion {
  import PucketDescriptor._

  type HigherType = Thrift
  type S = Class[_]

  val schemaClassKey = "thriftSchemaClass"

  private def validate[T <: HigherType](descriptorString: String): Throwable \/ (Class[T], CompressionCodecName, Option[PucketPartitioner[T]]) =
    for {
      underlying <- parseDescriptor[T](descriptorString)
      schema <- extractValue(underlying._1, schemaClassKey)
      schemaClass <- \/.fromTryCatchNonFatal(Class.forName(schema).asInstanceOf[Class[T]])
    } yield (schemaClass, underlying._2, underlying._3)

  /** @inheritdoc */
  override def apply[T <: HigherType](expectedSchemaClass: S, descriptorString: String): Throwable \/ ThriftPucketDescriptor[T] =
    for {
      underlying <- validate[T](descriptorString)
      _ <- if (underlying._1 == expectedSchemaClass) ().right
      else new RuntimeException("Expected schema class does not match given descriptor").left
    } yield new ThriftPucketDescriptor[T](underlying._1, underlying._2, underlying._3)

  /** @inheritdoc */
  override def apply[T <: HigherType](descriptorString: String): Throwable \/ ThriftPucketDescriptor[T] =
    validate[T](descriptorString).map(x => new ThriftPucketDescriptor[T](x._1, x._2, x._3))
}


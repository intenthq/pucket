package com.intenthq.pucket.thrift

import com.intenthq.pucket.javacompat.thrift.ThriftPucketInstantiator
import com.intenthq.pucket.{PucketDescriptorCompanion, PucketDescriptor}
import com.intenthq.pucket.util.PucketPartitioner
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.json4s.JsonDSL._

import scalaz.\/
import scalaz.syntax.either._

case class ThriftPucketDescriptor[T <: Thrift](schemaClass: Class[T],
                                               override val compression: CompressionCodecName,
                                               override val partitioner: Option[PucketPartitioner[T]] = None) extends PucketDescriptor[T] {
  import ThriftPucketDescriptor._

  override def json = (schemaClassKey -> schemaClass.getName) ~ commonJson

  override def instantiatorClass: Class[_] = classOf[ThriftPucketInstantiator]
}

object ThriftPucketDescriptor extends PucketDescriptorCompanion {

  import PucketDescriptor._

  type HigherType = Thrift
  type V = Class[_]

  val schemaClassKey = "thriftSchemaClass"

  private def validate[T <: Thrift](descriptorString: String): Throwable \/ (Class[T], CompressionCodecName, Option[PucketPartitioner[T]]) =
    for {
      underlying <- parseDescriptor[T](descriptorString)
      schema <- extractValue(underlying._1, schemaClassKey)
      schemaClass <- \/.fromTryCatchNonFatal(Class.forName(schema).asInstanceOf[Class[T]])
    } yield (schemaClass, underlying._2, underlying._3)

  override def apply[T <: Thrift](expectedSchemaClass: Class[_], descriptorString: String): Throwable \/
                                                                                            ThriftPucketDescriptor[T] =
    for {
      underlying <- validate[T](descriptorString)
      _ <- if (underlying._1 == expectedSchemaClass) ().right
      else new RuntimeException("sdfsfd").left
    } yield new ThriftPucketDescriptor[T](underlying._1, underlying._2, underlying._3)

  override def apply[T <: Thrift](descriptorString: String): Throwable \/ ThriftPucketDescriptor[T] =
    validate[T](descriptorString).map(x => new ThriftPucketDescriptor[T](x._1, x._2, x._3))
}


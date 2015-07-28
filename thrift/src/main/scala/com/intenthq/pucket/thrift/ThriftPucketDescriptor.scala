package com.intenthq.pucket.thrift

import com.intenthq.pucket.PucketDescriptor
import com.intenthq.pucket.util.Partitioner
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.json4s.JsonDSL._

import scalaz.\/
import scalaz.syntax.either._

case class ThriftPucketDescriptor[T <: Thrift](schemaClass: Class[T],
                                               override val compression: CompressionCodecName,
                                               override val partitioner: Option[Partitioner[T]] = None) extends PucketDescriptor[T] {
  import ThriftPucketDescriptor._

  override def json = (schemaClassKey -> schemaClass.getName) ~ commonJson
}

object ThriftPucketDescriptor {
  import PucketDescriptor._

  val schemaClassKey = "thriftSchemaClass"

  def apply[T <: Thrift](expectedSchemaClass: Class[T], descriptorString: String): Throwable \/ ThriftPucketDescriptor[T] =
    for {
      underlying <- parseDescriptor[T](descriptorString)
      schema <- extractValue(underlying._1, schemaClassKey)
      schemaClass <- \/.fromTryCatchNonFatal(Class.forName(schema).asInstanceOf[Class[T]])
      _ <- if (schemaClass == expectedSchemaClass) ().right
            else new RuntimeException("sdfsfd").left
    } yield new ThriftPucketDescriptor[T](schemaClass, underlying._2, underlying._3)

}


package com.intenthq.pucket.thrift

import com.intenthq.pucket._
import com.intenthq.pucket.reader.Reader
import com.intenthq.pucket.thrift.writer.ThriftWriter
import com.intenthq.pucket.util.PucketPartitioner
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.thrift.ThriftReadSupport

import scalaz.\/

/** A Thrift type of pucket.
  *
  * @param path path to the pucket
  * @param fs hadoop filesystem instance
  * @param descriptor the pucket descriptor. See [[com.intenthq.pucket.thrift.ThriftPucketDescriptor]]
  * @tparam T The data type the pucket will contain
  */
case class ThriftPucket[T <: Thrift] private (override val path: Path,
                                              override val fs: FileSystem,
                                              override val descriptor: ThriftPucketDescriptor[T],
                                              override val blockSize: Int) extends Pucket[T] {
  /** @inheritdoc */
  override def writer: Throwable \/ ThriftWriter[T] =
    ThriftWriter(descriptor.schemaClass,
                 filename,
                 descriptor.compression,
                 blockSize,
                 conf)

  /** @inheritdoc */
  override def reader(filter: Option[Filter]): Throwable \/ Reader[T] =
    Reader(fs, path, new ThriftReadSupport[T], filter)

  /** @inheritdoc */
  override protected def newInstance(newPath: Path): Pucket[T] = ThriftPucket[T](newPath, fs, descriptor, blockSize)
}

/** Factory for [[com.intenthq.pucket.thrift.ThriftPucket]] */
object ThriftPucket extends PucketCompanion {
  import Pucket._

  type HigherType = Thrift
  type V = Class[_ <: Thrift]
  type DescriptorType[T <: HigherType] = ThriftPucketDescriptor[T]

  def getDescriptor[T <: HigherType](schemaSpec: V,
                    compression: CompressionCodecName,
                    partitioner: Option[PucketPartitioner[T]]) = ThriftPucketDescriptor[T](schemaSpec.asInstanceOf[Class[T]], compression, partitioner)

  override def getDescriptorSchemaSpec[T <: HigherType](descriptor: ThriftPucketDescriptor[T]) = descriptor.schemaClass


  /** @inheritdoc */
  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              descriptor: DescriptorType[T],
                              blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => ThriftPucket[T](path, fs, descriptor, blockSize))

  /** @inheritdoc */
  def apply[T <: HigherType](path: Path,
                             fs: FileSystem,
                             expectedSchemaClass: V,
                             blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- ThriftPucketDescriptor[T](expectedSchemaClass.asInstanceOf[Class[T]], metadata)
    } yield ThriftPucket(path, fs, descriptor, blockSize)
}

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
import scalaz.syntax.either._

/** A Thrift type of pucket.
  *
  * @param path path to the pucket
  * @param fs hadoop filesystem instance
  * @param descriptor the pucket descriptor. See [[com.intenthq.pucket.thrift.ThriftPucketDescriptor]]
  * @tparam T The data type the pucket will contain
  */
case class ThriftPucket[T <: Thrift] private (override val path: Path,
                                              override val fs: FileSystem,
                                              override val descriptor: ThriftPucketDescriptor[T]) extends Pucket[T] {
  /** @inheritdoc */
  override def writer: Throwable \/ ThriftWriter[T] =
    ThriftWriter(descriptor.schemaClass,
                 filename,
                 descriptor.compression,
                 defaultBlockSize,
                 conf)

  /** @inheritdoc */
  override def reader(filter: Option[Filter]): Throwable \/ Reader[T] =
    Reader(fs, path, new ThriftReadSupport[T], filter)

  /** @inheritdoc */
  override protected def newInstance(newPath: Path): Pucket[T] = ThriftPucket[T](newPath, fs, descriptor)
}

/** Factory for [[com.intenthq.pucket.thrift.ThriftPucket]] */
object ThriftPucket extends PucketCompanion {
  import Pucket._

  type HigherType = Thrift
  type V = Class[_ <: Thrift]
  type DescriptorType[T <: HigherType] = ThriftPucketDescriptor[T]

  /** Find an existing pucket or create one if it does not exist
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param schemaClass thrift class which provides data schema
    * @param compression parquet compression codec to use
    * @param partitioner optional partitioning scheme
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the pucket
    */
  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    schemaClass: Class[T],
                                    compression: CompressionCodecName,
                                    partitioner: Option[PucketPartitioner[T]] = None): Throwable \/ Pucket[T] =
    findOrCreate(path, fs, ThriftPucketDescriptor[T](schemaClass, compression, partitioner))

  /** @inheritdoc */
  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    descriptor: DescriptorType[T]): Throwable \/ Pucket[T] =
    for {
      pucket <- apply[T](path, fs, descriptor.schemaClass).fold(_ => create[T](path, fs, descriptor), _.right)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket

  /** Create a new pucket
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param schemaClass thrift class which provides data schema
    * @param compression parquet compression codec to use
    * @param partitioner optional partitioning scheme
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the new pucket
    */
  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              schemaClass: Class[T],
                              compression: CompressionCodecName,
                              partitioner: Option[PucketPartitioner[T]] = None): Throwable \/ Pucket[T] =
    create(path, fs, ThriftPucketDescriptor[T](schemaClass, compression, partitioner))

  /** @inheritdoc */
  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              descriptor: DescriptorType[T]): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => ThriftPucket[T](path, fs, descriptor))

  /** @inheritdoc */
  def apply[T <: HigherType](path: Path, fs: FileSystem, expectedSchemaClass: V): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- ThriftPucketDescriptor[T](expectedSchemaClass.asInstanceOf[Class[T]], metadata)
    } yield ThriftPucket(path, fs, descriptor)
}

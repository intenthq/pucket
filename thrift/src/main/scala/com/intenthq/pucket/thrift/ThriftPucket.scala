package com.intenthq.pucket.thrift

import com.intenthq.pucket._
import com.intenthq.pucket.util.PucketPartitioner
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.thrift.ThriftReadSupport
import org.apache.parquet.thrift.ThriftParquetReader

import scalaz.\/
import scalaz.syntax.either._

case class ThriftPucket[T <: Thrift] private (override val path: Path,
                                              override val fs: FileSystem,
                                              override val descriptor: ThriftPucketDescriptor[T]) extends Pucket[T] {
  override def writer: Throwable \/ ThriftWriter[T] =
    ThriftWriter(descriptor.schemaClass,
                 filename,
                 descriptor.compression,
                 defaultBlockSize,
                 conf)

  override def reader(filter: Option[Filter]): ParquetReader[T] = {
    val readerBuilder = ThriftParquetReader.build[T](path).withConf(conf)
    filter.fold(readerBuilder)(readerBuilder.withFilter).build()
  }

  override def newInstance(newPath: Path): Pucket[T] = ThriftPucket[T](newPath, fs, descriptor)

  override def readSupportClass: Class[ThriftReadSupport[T]] = classOf[ThriftReadSupport[T]]
}


object ThriftPucket extends PucketCompanion {
  import Pucket._

  type HigherType = Thrift
  type V = Class[_ <: Thrift]
  type DescriptorType[T <: HigherType] = ThriftPucketDescriptor[T]

  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    schemaClass: Class[T],
                                    compression: CompressionCodecName,
                                    partitioner: Option[PucketPartitioner[T]] = None): Throwable \/ Pucket[T] =
    findOrCreate(path, fs, ThriftPucketDescriptor[T](schemaClass, compression, partitioner))

  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    descriptor: DescriptorType[T]): Throwable \/ Pucket[T] =
    for {
      pucket <- apply[T](path, fs, descriptor.schemaClass).fold(_ => create[T](path, fs, descriptor), _.right)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket


  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              schemaClass: Class[T],
                              compression: CompressionCodecName,
                              partitioner: Option[PucketPartitioner[T]] = None): Throwable \/ Pucket[T] =
    create(path, fs, ThriftPucketDescriptor[T](schemaClass, compression, partitioner))

  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              descriptor: DescriptorType[T]): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => ThriftPucket[T](path, fs, descriptor))

  def apply[T <: HigherType](path: Path, fs: FileSystem, expectedSchemaClass: V): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- ThriftPucketDescriptor[T](expectedSchemaClass.asInstanceOf[Class[T]], metadata)
    } yield ThriftPucket(path, fs, descriptor)
}

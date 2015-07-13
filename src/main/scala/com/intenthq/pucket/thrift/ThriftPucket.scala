package com.intenthq.pucket.thrift

import com.intenthq.pucket._
import com.intenthq.pucket.util.Partitioner
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
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
}

object ThriftPucket {
  import Pucket._

  def findOrCreate[T <: Thrift](path: Path,
                      fs: FileSystem,
                      schemaClass: Class[T],
                      compression: CompressionCodecName,
                      partitioner: Option[Partitioner[T]] = None): Throwable \/ Pucket[T] =
    findOrCreate(path, fs, ThriftPucketDescriptor[T](schemaClass, compression, partitioner))

  def findOrCreate[T <: Thrift](path: Path,
                      fs: FileSystem,
                      descriptor: ThriftPucketDescriptor[T]): Throwable \/ Pucket[T] =
    for {
      pucket <- apply[T](path, fs, descriptor.schemaClass).fold(_ => create[T](path, fs, descriptor), _.right)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket


  def create[T <: Thrift](path: Path,
                fs: FileSystem,
                schemaClass: Class[T],
                compression: CompressionCodecName,
                partitioner: Option[Partitioner[T]] = None): Throwable \/ Pucket[T] =
    create(path, fs, ThriftPucketDescriptor[T](schemaClass, compression, partitioner))

  def create[T <: Thrift](path: Path,
                fs: FileSystem,
                descriptor: ThriftPucketDescriptor[T]): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => ThriftPucket[T](path, fs, descriptor))

  def apply[T <: Thrift](path: Path, fs: FileSystem, expectedSchemaClass: Class[T]): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- ThriftPucketDescriptor[T](expectedSchemaClass, metadata)
    } yield ThriftPucket(path, fs, descriptor)
}

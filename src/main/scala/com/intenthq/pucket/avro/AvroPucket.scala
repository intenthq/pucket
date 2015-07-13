package com.intenthq.pucket.avro

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.util.Partitioner
import com.intenthq.pucket.writer.Writer
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/
import scalaz.syntax.either._

case class AvroPucket[T <: IndexedRecord] private (override val path: Path,
                                                    override val fs: FileSystem,
                                                    override val descriptor: AvroPucketDescriptor[T]) extends Pucket[T] {

  override def writer: Throwable \/ Writer[T, Throwable] =
    AvroWriter[T](descriptor.schema,
                  filename,
                  descriptor.compression,
                  defaultBlockSize,
                  conf)

  override def reader(filter: Option[Filter]): ParquetReader[T] = {
    val readerBuilder = AvroParquetReader.builder[T](path).withConf(conf)
    filter.fold(readerBuilder)(readerBuilder.withFilter).build()
  }

  override def newInstance(newPath: Path): Pucket[T] = AvroPucket(newPath, fs, descriptor)
}

object AvroPucket {
  import Pucket._

  def findOrCreate[T <: IndexedRecord](path: Path,
                      fs: FileSystem,
                      schema: Schema,
                      compression: CompressionCodecName,
                      partitioner: Option[Partitioner[T]] = None): Throwable \/ Pucket[T] =
    findOrCreate(path, fs, AvroPucketDescriptor[T](schema, compression, partitioner))

  def findOrCreate[T <: IndexedRecord](path: Path,
                      fs: FileSystem,
                      descriptor: AvroPucketDescriptor[T]): Throwable \/ Pucket[T] =
    for {
      pucket <- apply[T](path, fs, descriptor.schema).fold(_ => create[T](path, fs, descriptor), _.right)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket


  def create[T <: IndexedRecord](path: Path,
                fs: FileSystem,
                schema: Schema,
                compression: CompressionCodecName,
                partitioner: Option[Partitioner[T]] = None): Throwable \/ Pucket[T] =
    create(path, fs, AvroPucketDescriptor[T](schema, compression, partitioner))

  def create[T <: IndexedRecord](path: Path,
                fs: FileSystem,
                descriptor: AvroPucketDescriptor[T]): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => AvroPucket[T](path, fs, descriptor))

  def apply[T <: IndexedRecord](path: Path, fs: FileSystem, expectedSchema: Schema): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- AvroPucketDescriptor[T](expectedSchema, metadata)
    } yield AvroPucket(path, fs, descriptor)
}

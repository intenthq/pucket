package com.intenthq.pucket.avro

import com.intenthq.pucket.reader.Reader
import com.intenthq.pucket.util.PucketPartitioner
import com.intenthq.pucket.writer.Writer
import com.intenthq.pucket.{Pucket, PucketCompanion}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.filter2.compat.FilterCompat.Filter
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

  override def reader(filter: Option[Filter]): Throwable \/ Reader[T] =
    Reader(fs, path, new AvroReadSupport[T], filter)

  override def newInstance(newPath: Path): Pucket[T] = AvroPucket(newPath, fs, descriptor)

  override val readSupportClass: Class[AvroReadSupport[T]] = classOf[AvroReadSupport[T]]
}

object AvroPucket extends PucketCompanion {
  import Pucket._

  type HigherType = IndexedRecord
  type V = Schema
  type DescriptorType[T <: HigherType] = AvroPucketDescriptor[T]

  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    schema: Schema,
                                    compression: CompressionCodecName,
                                    partitioner: Option[PucketPartitioner[T]] = None): Throwable \/ Pucket[T] =
    findOrCreate(path, fs, AvroPucketDescriptor[T](schema, compression, partitioner))

  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    descriptor: DescriptorType[T]): Throwable \/ Pucket[T] =
    for {
      pucket <- apply[T](path, fs, descriptor.schema).fold(_ => create[T](path, fs, descriptor), _.right)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket


  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              schema: Schema,
                              compression: CompressionCodecName,
                              partitioner: Option[PucketPartitioner[T]] = None): Throwable \/ Pucket[T] =
    create[T](path, fs, AvroPucketDescriptor[T](schema, compression, partitioner))

  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              descriptor: DescriptorType[T]): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => AvroPucket[T](path, fs, descriptor))

  def apply[T <: HigherType](path: Path, fs: FileSystem, expectedSchema: V): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- AvroPucketDescriptor[T](expectedSchema, metadata)
    } yield AvroPucket(path, fs, descriptor)
}

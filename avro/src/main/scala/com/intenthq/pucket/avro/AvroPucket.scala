package com.intenthq.pucket.avro

import com.intenthq.pucket.avro.writer.AvroWriter
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

import scala.language.higherKinds
import scalaz.\/
/** An Avro type of pucket.
  *
  * @param path path to the pucket
  * @param fs hadoop filesystem instance
  * @param descriptor the pucket descriptor. See [[com.intenthq.pucket.avro.AvroPucketDescriptor]]
  * @tparam T The data type the pucket will contain
  */
case class AvroPucket[T <: IndexedRecord] private (override val path: Path,
                                                   override val fs: FileSystem,
                                                   override val descriptor: AvroPucketDescriptor[T],
                                                   override val blockSize: Int) extends Pucket[T] {

  /** @inheritdoc */
  override def writer: Throwable \/ Writer[T, Throwable] =
    AvroWriter[T](descriptor.schema,
                  filename,
                  descriptor.compression,
                  blockSize,
                  conf)

  /** @inheritdoc */
  override def reader(filter: Option[Filter]): Throwable \/ Reader[T] =
    Reader(fs, path, new AvroReadSupport[T], filter)

  /** @inheritdoc */
  override protected def newInstance(newPath: Path): Pucket[T] = AvroPucket(newPath, fs, descriptor, blockSize)
}

/** Factory for [[com.intenthq.pucket.avro.AvroPucket]] */
object AvroPucket extends PucketCompanion {
  import Pucket._

  type HigherType = IndexedRecord
  type V = Schema
  type DescriptorType[T <: HigherType] = AvroPucketDescriptor[T]

  def getDescriptor[T <: HigherType](schemaSpec: V,
                                     compression: CompressionCodecName,
                                     partitioner: Option[PucketPartitioner[T]]) = AvroPucketDescriptor[T](schemaSpec, compression, partitioner)

  def getDescriptorSchemaSpec[T <: HigherType](descriptor: AvroPucketDescriptor[T]) = descriptor.schema

  /** @inheritdoc */
  override def create[T <: HigherType](path: Path,
                                       fs: FileSystem,
                                       descriptor: DescriptorType[T],
                                       blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T] =
    writeMeta(path, fs, descriptor).map(_ => AvroPucket[T](path, fs, descriptor, blockSize))

  /** @inheritdoc */
  override def apply[T <: HigherType](path: Path,
                                      fs: FileSystem,
                                      expectedSchema: V,
                                      blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T] =
    for {
      metadata <- readMeta(path, fs)
      descriptor <- AvroPucketDescriptor[T](expectedSchema, metadata)
    } yield AvroPucket(path, fs, descriptor, blockSize)
}

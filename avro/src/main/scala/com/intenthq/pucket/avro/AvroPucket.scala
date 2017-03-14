package com.intenthq.pucket.avro

import scala.language.higherKinds

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

import scalaz.\/
import scalaz.syntax.either._
/** A Avro type of pucket.
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

  /** Find an existing pucket or create one if it does not exist
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param schema avro schema instance for the data type
    * @param compression parquet compression codec to use
    * @param partitioner optional partitioning scheme
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the pucket
    */
  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    schema: Schema,
                                    compression: CompressionCodecName,
                                    partitioner: Option[PucketPartitioner[T]]): Throwable \/ Pucket[T] =
    findOrCreate(path, fs, AvroPucketDescriptor[T](schema, compression, partitioner))

  /** @inheritdoc */
  override def findOrCreate[T <: HigherType](path: Path,
                                             fs: FileSystem,
                                             descriptor: DescriptorType[T],
                                             blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T] =
    for {
      pucket <- apply[T](path, fs, descriptor.schema, blockSize).
                  fold(_ => create[T](path, fs, descriptor, blockSize), _.right)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket

  /** Create a new pucket
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param schema avro schema instance for the data type
    * @param compression parquet compression codec to use
    * @param partitioner optional partitioning scheme
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the new pucket
    */
  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              schema: Schema,
                              compression: CompressionCodecName,
                              partitioner: Option[PucketPartitioner[T]]): Throwable \/ Pucket[T] =
    create[T](path, fs, AvroPucketDescriptor[T](schema, compression, partitioner))

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

package com.intenthq.pucket.avro.writer

import com.intenthq.pucket.avro.writer
import com.intenthq.pucket.writer.Writer
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/

/** Functional wrapper for avro parquet writer */
case class AvroWriter[T <: IndexedRecord] private (writer: ParquetWriter[T]) extends Writer[T, Throwable] {

  /** @inheritdoc */
  override def close: Throwable \/ Unit = \/.fromTryCatchNonFatal(writer.close())

  /** @inheritdoc */
  override def write(data: T, checkPoint: Long): Throwable \/ Writer[T, Throwable] =
    \/.fromTryCatchNonFatal(writer.write(data)).map(_ => this)
}

/** Factory object for [[AvroWriter]] */
object AvroWriter {
  /** Create a new avro writer
   *
   * @param schema avro schema
   * @param path path to the file for writing
   * @param compression compression codec
   * @param blockSize parquet block size
   * @param conf hadoop configuration
   * @tparam T type of data to be written
   * @return a new avro writer or a creation error
   */
  def apply[T <: IndexedRecord](schema: Schema,
                                path: Path,
                                compression: CompressionCodecName,
                                blockSize: Int,
                                conf: Configuration): Throwable \/ AvroWriter[T] =
    \/.fromTryCatchNonFatal(
      AvroWriter(
        AvroParquetWriter.
          builder[T](path).
          withSchema(schema).
          withConf(conf).
          withCompressionCodec(compression).
          withRowGroupSize(blockSize).
          build()
      )
    )
}

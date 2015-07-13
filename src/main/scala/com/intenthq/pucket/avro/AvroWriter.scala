package com.intenthq.pucket.avro

import com.intenthq.pucket.writer.Writer
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/

case class AvroWriter[T <: IndexedRecord] private (writer: ParquetWriter[T]) extends Writer[T, Throwable] {

  override def close: Throwable \/ Unit = \/.fromTryCatchNonFatal(writer.close())

  override def write(data: T, checkPoint: Long): Throwable \/ Writer[T, Throwable] =
    \/.fromTryCatchNonFatal(writer.write(data)).map(_ => this)
}

object AvroWriter {
  def apply[T <: IndexedRecord](schema: Schema,
                                path: Path,
                                compression: CompressionCodecName,
                                blockSize: Int,
                                conf: Configuration): Throwable \/ AvroWriter[T] =
    \/.fromTryCatchNonFatal(
      AvroWriter(
        new AvroParquetWriter[T](
          path,
          schema,
          compression,
          blockSize,
          ParquetWriter.DEFAULT_PAGE_SIZE,
          ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
          conf)
      )
    )
}

package com.intenthq.pucket.thrift

import com.intenthq.pucket.writer.Writer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.thrift.ThriftParquetWriter

import scalaz.\/

case class ThriftWriter[T] private (writer: ParquetWriter[T]) extends Writer[T, Throwable] {

  override def write(data: T, checkPoint: Long = 0): Throwable \/ ThriftWriter[T] =
    \/.fromTryCatchNonFatal(writer.write(data)).map(_ => this)

  override def close: Throwable \/ Unit = \/.fromTryCatchNonFatal(writer.close())

}

object ThriftWriter {
  def apply[T <: Thrift](schemaClass: Class[T],
                         path: Path,
                         compression: CompressionCodecName,
                         blockSize: Int,
                         conf: Configuration): Throwable \/ ThriftWriter[T] =
    \/.fromTryCatchNonFatal(
      ThriftWriter(
        new ThriftParquetWriter[T](
          path,
          schemaClass,
          compression,
          blockSize,
          ParquetWriter.DEFAULT_PAGE_SIZE,
          ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
          ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
          conf)
      )
    )
}

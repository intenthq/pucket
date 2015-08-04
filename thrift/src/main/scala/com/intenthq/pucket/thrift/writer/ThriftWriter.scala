package com.intenthq.pucket.thrift.writer

import com.intenthq.pucket.thrift._
import com.intenthq.pucket.writer.Writer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.thrift.ThriftParquetWriter

import scalaz.\/

/** Functional wrapper for thrift parquet writer */
case class ThriftWriter[T] private (writer: ParquetWriter[T]) extends Writer[T, Throwable] {

  /** @inheritdoc */
  override def write(data: T, checkPoint: Long = 0): Throwable \/ ThriftWriter[T] =
    \/.fromTryCatchNonFatal(writer.write(data)).map(_ => this)

  /** @inheritdoc */
  override def close: Throwable \/ Unit = \/.fromTryCatchNonFatal(writer.close())

}

/** Factory object for [[ThriftWriter]] */
object ThriftWriter {

  /** Create a new thrift writer
    *
    * @param schemaClass the thrift schema class
    * @param path path to the file for writing
    * @param compression compression codec
    * @param blockSize parquet block siz
    * @param conf hadoop configuration
    * @tparam T type of data to be written
    * @return a new thrift writer or a creation error
    */
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

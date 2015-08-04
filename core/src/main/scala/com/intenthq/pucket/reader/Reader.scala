package com.intenthq.pucket.reader

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.util.HadoopUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.filter2.compat.{FilterCompat, RowGroupFilter}
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.{Footer, InternalParquetRecordReaderWrapper, ParquetFileReader}

import scala.collection.JavaConversions._
import scalaz.\/
import scalaz.syntax.either._

/** Functional wrapper around parquet's internal reader
  * provides the ability to read from a partitioned pucket
  */
case class Reader[T] private (footers: List[Footer],
                              readSupport: ReadSupport[T],
                              filter: Filter,
                              conf: Configuration,
                              reader: InternalParquetRecordReaderWrapper[T]) {
  import Reader._

  /** Read data from the pucket in sequence
    * Will return a [[None]] if input is exhausted
    *
    * @return a tuple of the reader state and optional data
    *         or an error
    */
  def read: Throwable \/ (Option[T], Reader[T]) =
    if (reader.nextKeyValue())
      \/.fromTryCatchNonFatal((Some(reader.getCurrentValue), newInstance(footers, reader)))
    else if (footers.nonEmpty) for {
        _ <- \/.fromTryCatchNonFatal(reader.close())
        r <- initReader(footers, readSupport, filter, conf)
        ret <- \/.fromTryCatchNonFatal(
          (if (r._2.nextKeyValue()) Some(r._2.getCurrentValue)
           else None, newInstance(r._1, r._2))
        )
      } yield ret
    else (None, this).right

  /** Closes the underlying reader */
  def close: Throwable \/ Unit = \/.fromTryCatchNonFatal(reader.close())

  def newInstance(f: List[Footer], r: InternalParquetRecordReaderWrapper[T]): Reader[T] =
    Reader[T](f, readSupport, filter, conf, r)
  
}

/** Factory object for [[Reader]] */
object Reader {

  /** Create a new instance of the reader
   *
   * @param fs Hadoop filessystem instance
   * @param path path to the pucket
   * @param readSupport parquet read support path
   * @param filter parquet read filter
   * @tparam T type of data to be read
   * @return a new instance of the reader, or an error
   */
  def apply[T](fs: FileSystem, path: Path, readSupport: ReadSupport[T], filter: Option[Filter]): Throwable \/ Reader[T] = {
    val conf = fs.getConf
    val f = filter.getOrElse(FilterCompat.NOOP)
    for {
      statuses <-  HadoopUtil.fileStatuses(path, fs, Pucket.extension)
      footers <- \/.fromTryCatchNonFatal(ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(fs.getConf, statuses, false))
      reader <- initReader(footers.toList, readSupport, f, fs.getConf)
    } yield Reader[T](reader._1, readSupport, f, fs.getConf, reader._2)
  }

  private def initReader[T](footers: List[Footer],
                            readSupport: ReadSupport[T],
                            filter: Filter,
                            conf: Configuration): Throwable \/ (List[Footer], InternalParquetRecordReaderWrapper[T]) = {
    if (footers.nonEmpty) {
      \/.fromTryCatchNonFatal{
        val footer = footers.head
        val blocks = footer.getParquetMetadata.getBlocks
        val fileSchema = footer.getParquetMetadata.getFileMetaData.getSchema
        val filteredBlocks = RowGroupFilter.filterRowGroups(filter, blocks, fileSchema)

        val reader = new InternalParquetRecordReaderWrapper[T](readSupport , filter)
        reader.initialize(fileSchema, footer.getParquetMetadata.getFileMetaData, footer.getFile, filteredBlocks, conf)

        (footers.drop(1), reader)
      }
    } else new RuntimeException("No footers left to read from").left
  }
}

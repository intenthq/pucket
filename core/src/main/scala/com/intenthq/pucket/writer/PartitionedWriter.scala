package com.intenthq.pucket.writer

import com.intenthq.pucket._
import com.intenthq.pucket.writer.PartitionedWriterFunctions.Writers
import org.apache.hadoop.fs.Path

import scalaz.\/
import scalaz.syntax.either._

/** Partitioned parquet writer
  * Allows a pucket to be written to with a partitioning scheme
  *
  * */
case class PartitionedWriter[T] private (override val pucket: Pucket[T],
                                         writers: Writers[T, Throwable],
                                         override val writerCacheSize: Int) extends Writer[T, Throwable] with
                                                                                    PartitionedWriterFunctions[T, Throwable, PartitionedWriter[T]] {
  /** @inheritdoc */
  override def newInstance(writers: Writers[T, Throwable]): PartitionedWriter[T] =
    PartitionedWriter(pucket, writers, writerCacheSize)

  /** @inheritdoc */
  override def newWriter(partition: Path, checkPoint: Long): Throwable \/ Writer[T, Throwable] =
    pucket.subPucket(partition).flatMap(_.writer)


  /** @inheritdoc */
  override def close: Throwable \/ Unit =
    writers.partitions.values.foldLeft[Throwable \/ Unit](().right)( (acc, writer) =>
      acc.fold(x => writer.close.
        fold(y => combineExceptions(x, y).left, _ => acc), _ => writer.close)
    )
}

/** Factory object for [[com.intenthq.pucket.writer.PartitionedWriter]] */
object PartitionedWriter {

  /** Create a new instance of the partitioned writer
   *
   * @param pucket the pucket to be written to
   * @param writerCacheSize the size of the writer cache
   * @tparam T the data type to be written
   * @return a new partitioned writer
   */
  def apply[T](pucket: Pucket[T], writerCacheSize: Int = 100): PartitionedWriter[T] =
    PartitionedWriter(pucket, Writers[T, Throwable](Map[String, Writer[T, Throwable]](), Map[Long, String]()), writerCacheSize)
}

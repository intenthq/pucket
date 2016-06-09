package com.intenthq.pucket.writer

import com.intenthq.pucket._
import com.intenthq.pucket.writer.PartitionedWriterFunctions.Writers
import org.apache.hadoop.fs.Path

import scalaz.\/
import scalaz.syntax.either._

/** Incremental partitioned parquet writer
  * Provides incremental writing for partitioned puckets
  * Any error will return with the minimum checkpoint of the current set of writers
  */
case class IncrementalPartitionedWriter[T] private (pucket: Pucket[T],
                                                    writers: Writers[T, (Long, Throwable)],
                                                    maxWrites: Long,
                                                    override val writerCacheSize: Int) extends Writer[T, (Long, Throwable)] with
                                                                                           PartitionedWriterFunctions[T, (Long, Throwable), IncrementalPartitionedWriter[T]] {
  type Error = (Long, Throwable)

  /** @inheritdoc */
  override def newInstance(writers: Writers[T, Error]): IncrementalPartitionedWriter[T] =
    IncrementalPartitionedWriter(pucket, writers, maxWrites, writerCacheSize)

  /** @inheritdoc */
  override def newWriter(partition: Path, checkPoint: Long): Error \/ Writer[T, Error] =
    pucket.subPucket(partition).fold[Error \/ Writer[T, Error]](ex => (minCheckpoint, ex).left, IncrementalWriter(checkPoint, _, maxWrites))

  /** @inheritdoc */
  override def close: Error \/ Unit =
    writers.partitions.values.foldLeft[Error \/ Unit](().right)((acc, writer) =>
      acc.fold(x => writer.close.
        fold( y => (math.min(x._1, y._1), combineExceptions(x._2, y._2)).left,
              _ => acc), _ => writer.close)
    )

  /** Used to find the lowest checkpoint for
    * which data is confirmed to be written
    *
    * @return the lowest checkpoint
    */
  def minCheckpoint: Long = writers.partitions.values.map(_.checkPoint).min
}

/** Factory object for [[com.intenthq.pucket.writer.IncrementalPartitionedWriter]] */
object IncrementalPartitionedWriter {
  /** Create a new instance of the incremental partitioned writer
    *
    * @param pucket the pucket to be written to
    * @param maxWrites the maximum number of writes before
    *                  the current parquet file gets rolled
    * @param writerCacheSize the size of the writer cache
    * @tparam T the data type to be written
    * @return a new incremental partitioned writer
    */
  def apply[T](pucket: Pucket[T], maxWrites: Long, writerCacheSize: Int = 100): IncrementalPartitionedWriter[T] =
    IncrementalPartitionedWriter(pucket, Writers(Map[String, IncrementalWriter[T]](), Map[Long, String]()), maxWrites, writerCacheSize)
}

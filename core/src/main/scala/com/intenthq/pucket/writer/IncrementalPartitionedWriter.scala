package com.intenthq.pucket.writer

import com.intenthq.pucket._
import com.intenthq.pucket.writer.PartitionedWriterFunctions.Writers

import scalaz.\/
import scalaz.syntax.either._

case class IncrementalPartitionedWriter[T] private (pucket: Pucket[T],
                                                    writers: Writers[T, (Long, Throwable)],
                                                    maxSize: Long) extends Writer[T, (Long, Throwable)] with
                                                                           PartitionedWriterFunctions[T, (Long, Throwable), IncrementalPartitionedWriter[T]] {
  type Error = (Long, Throwable)

  override val maxWriters = 100

  override def write(data: T, checkPoint: Long): Error \/ IncrementalPartitionedWriter[T] =
    writePartition(data, checkPoint, pucket.partition(data).leftMap((minCheckpoint, _)))

  override def newInstance(writers: Writers[T, Error]): IncrementalPartitionedWriter[T] =
    IncrementalPartitionedWriter(pucket, writers, maxSize)

  override def newWriter(partition: Pucket[T], checkPoint: Long): Error \/ Writer[T, Error] =
    IncrementalWriter(checkPoint, partition, maxSize)

  override def close: Error \/ Unit =
    writers.partitions.values.foldLeft[Error \/ Unit](().right)((acc, writer) =>
      acc.fold(x => writer.close.
        fold( y => (math.min(x._1, y._1), combineExceptions(x._2, y._2)).left,
              _ => acc), _ => writer.close)
    )

  def minCheckpoint: Long = writers.partitions.values.map(_.checkPoint).min
}

object IncrementalPartitionedWriter {
  def apply[T](pucket: Pucket[T], maxSize: Long): IncrementalPartitionedWriter[T] =
    IncrementalPartitionedWriter(pucket, Writers(Map[String, IncrementalWriter[T]](), Map[Long, String]()), maxSize)
}

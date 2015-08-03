package com.intenthq.pucket.writer

import com.intenthq.pucket._
import com.intenthq.pucket.writer.PartitionedWriterFunctions.Writers

import scalaz.\/
import scalaz.syntax.either._

case class PartitionedWriter[T] private (pucket: Pucket[T],
                                         writers: Writers[T, Throwable],
                                         override val writerCache: Int) extends Writer[T, Throwable] with
                                                                               PartitionedWriterFunctions[T, Throwable, PartitionedWriter[T]] {
  override def write(data: T, checkPoint: Long = 0): Throwable \/ PartitionedWriter[T] =
    writePartition(data, checkPoint, pucket.partition(data))

  override def newInstance(writers: Writers[T, Throwable]): PartitionedWriter[T] =
    PartitionedWriter(pucket, writers, writerCache)

  override def newWriter(partition: Pucket[T], checkPoint: Long): Throwable \/ Writer[T, Throwable] =
    partition.writer

  override def close: Throwable \/ Unit =
    writers.partitions.values.foldLeft[Throwable \/ Unit](().right)( (acc, writer) =>
      acc.fold(x => writer.close.
        fold(y => combineExceptions(x, y).left, _ => acc), _ => writer.close)
    )
}

object PartitionedWriter {
  def apply[T](pucket: Pucket[T], writerCache: Int = 100): PartitionedWriter[T] =
    PartitionedWriter(pucket, Writers[T, Throwable](Map[String, Writer[T, Throwable]](), Map[Long, String]()), writerCache)
}

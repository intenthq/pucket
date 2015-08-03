package com.intenthq.pucket.writer

import com.intenthq.pucket.Pucket

import scalaz.\/
import scalaz.syntax.either._

trait PartitionedWriterFunctions[T, Ex, WriterType] { self: Writer[T, Ex] =>
  import PartitionedWriterFunctions._
  def writers: Writers[T, Ex]
  def writerCache: Int

  def newInstance(writers: Writers[T, Ex]): WriterType

  def newWriter(partition: Pucket[T], checkPoint: Long): Ex \/ Writer[T, Ex]

  def writePartition(data: T,
                     checkPoint: Long,
                     partition: Ex \/ Pucket[T]): Ex \/ WriterType =
    partition.flatMap( p =>
     writers.partitions.
       get(p.id).
       map(_.write(data, checkPoint)).
       getOrElse(newWriter(p, checkPoint).flatMap(_.write(data, checkPoint)))
       flatMap(writer => writerCache(p.id, writer).map(newInstance))
    )

  def writerCache(partitionId: String, writer: Writer[T, Ex]): Ex \/ Writers[T, Ex] =
    if (writers.partitions.size < writerCache || writers.partitions.isDefinedAt(partitionId))
      Writers(writers.partitions + (partitionId -> writer),
              writers.lastUsed + (System.currentTimeMillis() -> partitionId)).right
    else {
      val oldestWriter = writers.lastUsed.toList.sortBy(_._1).head
      writers.partitions(oldestWriter._2).close.map( _ =>
        Writers((writers.partitions - oldestWriter._2) + (partitionId -> writer),
                (writers.lastUsed - oldestWriter._1) + (System.currentTimeMillis() -> partitionId))
      )
    }
}

object PartitionedWriterFunctions {
  case class Writers[T, A](partitions: Map[String, Writer[T, A]], lastUsed: Map[Long, String])
}

package com.intenthq.pucket.writer

import com.intenthq.pucket.Pucket

import scalaz.\/
import scalaz.syntax.either._

/** Partitioned writer mixin
  * provides partitioning to a writer type
  *
  * ==Overview==
  * This trait provides common partitioning functions to implementing
  * classes. It maintains a cache of open writer instances with a
  * configurable size.
  *
  * @tparam T type of data to be written
  * @tparam Ex type of error
  * @tparam ImplementingType implementing type to be used when
  *                          creating new instances
  */
trait PartitionedWriterFunctions[T, Ex, ImplementingType] { self: Writer[T, Ex] =>
  import PartitionedWriterFunctions._
  def writers: Writers[T, Ex]
  def writerCacheSize: Int

  /** Create a new instance of the partitioned writer
    *
    * @param writers new writer cache state to be
    *               included in the new instance
    * @return a new instance of a partitioned writer
    *         with the new state
    */
  def newInstance(writers: Writers[T, Ex]): ImplementingType

  /** Obtain a new writer for a partition
    *
    * @param partition the pucket instance for the partition
    * @param checkPoint the current checkpoint
    * @return a new writer for the partition or an error
    */
  def newWriter(partition: Pucket[T], checkPoint: Long): Ex \/ Writer[T, Ex]

  /** Write data to a partition
    * Finds a writer in the cache or creates a new one
    * then submits the writer back to the cache
    *
    * @param data the data to be written
    * @param checkPoint the current checkpoint to be passed
    *                   to the underlying writer
    * @param partition the pucket instance for the partition
    * @return a new instance of the partitioned writer
    *         complete with new state
    */
  def writePartition(data: T,
                     checkPoint: Long,
                     partition: Ex \/ Pucket[T]): Ex \/ ImplementingType =
    partition.flatMap( p =>
     writers.partitions.
       get(p.id).
       map(_.write(data, checkPoint)).
       getOrElse(newWriter(p, checkPoint).flatMap(_.write(data, checkPoint)))
       flatMap(writer => writerCache(p.id, writer).map(newInstance))
    )

  /** Add a new writer to the cache
    * Will update an existing writer if one for the same
    * partition already exists in cache.
    *
    * If a new writer needs to be cached and the cache
    * is full, the oldest one in the cache will be closed
    * and evicted.
    *
    * @param partitionId pucket identifier to use as a key in the cache
    * @param writer the writer instance to be cached
    * @return
    */
  def writerCache(partitionId: String, writer: Writer[T, Ex]): Ex \/ Writers[T, Ex] =
    if (writers.partitions.size < writerCacheSize || writers.partitions.isDefinedAt(partitionId))
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

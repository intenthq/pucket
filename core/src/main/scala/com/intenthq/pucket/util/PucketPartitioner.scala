package com.intenthq.pucket.util

import com.intenthq.pucket.Pucket

import scalaz.\/

/** Trait for implementing a pucket partitioner
 *
 * @tparam T type of data to be partitioned
 */
trait PucketPartitioner[T] {

  /** Partition a pucket
   *
   * @param data data to be used for partitioning
   * @param pucket pucket under which to store the data
   * @return a new pucket on a new path
   */
  def partition(data: T, pucket: Pucket[T]): Throwable \/ Pucket[T]
}

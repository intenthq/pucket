package com.intenthq.pucket.util

import org.apache.hadoop.fs.Path

/** Trait for implementing a pucket partitioner
 *
 * @tparam T type of data to be partitioned
 */
trait PucketPartitioner[T] {

  /** Partition a pucket
   *
   * @param data data to be used for partitioning
   * @return a the path of the path
   */
  def partition(data: T): Path
}

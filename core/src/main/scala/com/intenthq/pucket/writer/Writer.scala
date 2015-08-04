package com.intenthq.pucket.writer

import scalaz.\/

/** Trait for functional wrappers of parquet writers
  *
  * @tparam T type of data to be written
  * @tparam Ex type of error returned if write fails
  */
trait Writer[T, Ex] {

  /** Close the writer
   *
   * @return an error if the close fails or unit
   */
  def close: Ex \/ Unit

  /** Write data
   *
   * @param data the data to be written
   * @param checkPoint optional checkpoint to be maintained by the writer
   * @return the writer instance or an error instance if the write fails
   */
  def write(data: T, checkPoint: Long = 0): Ex \/ Writer[T, Ex]
  def checkPoint: Long = 0
  
  protected def combineExceptions(last: Throwable, current: Throwable): Throwable = {
    // TODO log here
    last.addSuppressed(current)
    last
  }
}

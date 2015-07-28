package com.intenthq.pucket.writer

import scalaz.\/

trait Writer[T, Ex] {

  def close: Ex \/ Unit
  def write(data: T, checkPoint: Long = 0): Ex \/ Writer[T, Ex]
  def checkPoint: Long = 0
  
  protected def combineExceptions(last: Throwable, current: Throwable): Throwable = {
    // TODO log here
    last.addSuppressed(current)
    last
  }
}

package com.intenthq.pucket.util

import scalaz.\/

object ExceptionUtil {
  def doThrow[T](thing: Throwable \/ T): T =
    thing.fold[T](throw _, identity)

  implicit class ThrowableEither[T](thing: Throwable \/ T) {
    def throwException = doThrow(thing)
  }
}



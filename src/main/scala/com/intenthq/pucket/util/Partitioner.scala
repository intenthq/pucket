package com.intenthq.pucket.util

import com.intenthq.pucket.Pucket

import scalaz.\/

trait Partitioner[T] {

  def partition(data: T, pucket: Pucket[T]): Throwable \/ Pucket[T]
}

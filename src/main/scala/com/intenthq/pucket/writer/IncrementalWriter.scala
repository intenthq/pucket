package com.intenthq.pucket.writer

import com.intenthq.pucket.Pucket

import scalaz.\/
import scalaz.syntax.either._

case class IncrementalWriter[T] private (override val checkPoint: Long,
                                         writeCounter: Long,
                                         writer: Writer[T, Throwable],
                                         pucket: Pucket[T],
                                         maxWrites: Long) extends Writer[T, (Long, Throwable)] {
  type Error = (Long, Throwable)

  def write(data: T, newCheckPoint: Long): Error \/ IncrementalWriter[T] = {
    if (writeCounter < maxWrites) append(data)
    else for {
      _ <- close
      newWriter <- pucket.writer.leftMap((checkPoint, _))
      _ <- newWriter.write(data).leftMap((checkPoint, _))
    } yield IncrementalWriter[T](newCheckPoint, 1, newWriter, pucket, maxWrites)
  }

  def close: Error \/ Unit =
    writer.close.leftMap((checkPoint, _))

  private def append(data: T): Error \/ IncrementalWriter[T] =
    writer.write(data).fold(
      th => (checkPoint, th).left,
      _ => IncrementalWriter[T](checkPoint, writeCounter + 1, writer, pucket, maxWrites).right
    )
}

object IncrementalWriter {
  def apply[T](checkPoint: Long,
               pucket: Pucket[T],
               maxSize: Long): (Long, Throwable) \/ IncrementalWriter[T] =
    pucket.writer.
      fold(th => (checkPoint, th).left, IncrementalWriter(checkPoint, 0, _, pucket, maxSize).right)
}

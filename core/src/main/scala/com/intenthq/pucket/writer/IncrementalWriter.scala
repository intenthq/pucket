package com.intenthq.pucket.writer

import com.intenthq.pucket.Pucket

import scalaz.\/
import scalaz.syntax.either._

/** Incremental functional writer class for parquet
  *
  * ==Overview==
  * Allows large files to be written to parquet in smaller incements.
  * Closes the underlying writer and creates a new file at a defined
  * writer count.
  *
  * Also keeps a checkpoint of when the last file was sucessfully
  * flushed to the filesystem so in the event of an error processing
  * can be restarted from that point.
  *
  * @tparam T type of data to be written
  */
case class IncrementalWriter[T] private (override val checkPoint: Long,
                                         writeCounter: Long,
                                         writer: Writer[T, Throwable],
                                         pucket: Pucket[T],
                                         maxWrites: Long) extends Writer[T, (Long, Throwable)] {
  type Error = (Long, Throwable)

  /** Write data incrementally
   *
   * @param data the data to be written
   * @param newCheckPoint the current progress of processing in the subject file
   * @return the writer instance or an error message with the checkpoint if the write fails
   */
  def write(data: T, newCheckPoint: Long): Error \/ IncrementalWriter[T] = {
    if (writeCounter < maxWrites) append(data)
    else for {
      _ <- close
      newWriter <- pucket.writer.leftMap((checkPoint, _))
      _ <- newWriter.write(data).leftMap((checkPoint, _))
    } yield IncrementalWriter[T](newCheckPoint, 1, newWriter, pucket, maxWrites)
  }

  /** @inheritdoc */
  def close: Error \/ Unit =
    writer.close.leftMap((checkPoint, _))

  private def append(data: T): Error \/ IncrementalWriter[T] =
    writer.write(data).fold(
      th => (checkPoint, th).left,
      _ => IncrementalWriter[T](checkPoint, writeCounter + 1, writer, pucket, maxWrites).right
    )
}

/** Factory object for [[com.intenthq.pucket.writer.IncrementalWriter]] */ 
object IncrementalWriter {

  /** Create a new instance of incremental writer
   * 
   * @param checkPoint the checkpoint progress through the file
   * @param pucket a pucket instance from which to obtain underlying writers
   * @param maxWrites the maximum number of writes before
   *                  the current parquet file gets rolled
   * @tparam T type of data to be written
   * @return a new writer instance or an error message
   *         with the checkpoint if the write fails
   */
  def apply[T](checkPoint: Long,
               pucket: Pucket[T],
               maxWrites: Long): (Long, Throwable) \/ IncrementalWriter[T] =
    pucket.writer.
      fold(th => (checkPoint, th).left, IncrementalWriter(checkPoint, 0, _, pucket, maxWrites).right)
}

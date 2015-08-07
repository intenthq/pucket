package com.intenthq.pucket.mapreduce

import com.intenthq.pucket._
import com.intenthq.pucket.util.ExceptionUtil._
import com.intenthq.pucket.writer.{PartitionedWriter, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.parquet.hadoop.util.ContextUtil._

import scalaz.\/
import scalaz.syntax.either._

/** Implementation of output format for hadoop
 *
 * @tparam T Type of data to be written
 */
class PucketOutputFormat[T] extends FileOutputFormat[Void, T] {
  import PucketOutputFormat._

  case class PucketRecordWriter(writer: Writer[T, Throwable]) extends RecordWriter[Void, T] {
    private var mutableWriter = writer

    override def write(key: Void, value: T): Unit =
      mutableWriter = mutableWriter.write(value).throwException

    override def close(context: TaskAttemptContext): Unit =
      mutableWriter.close.throwException
  }

  /** Create a pucket from the configuration settings
    * Uses reflection to instantiate a new instance of the pucket
    *
    * @param conf hadoop configuration instance
    * @param path path to the pucket
    * @param fs hadoop filesystem instance
    * @return a new pucket or error if validation fails
    */
  def getPucket(conf: Configuration, path: Path, fs: FileSystem): Throwable \/ Pucket[T] =
    for {
      instantiator <- \/.fromTryCatchNonFatal(
        Class.forName(conf.get(pucketInstantiatorKey)).
          newInstance().
          asInstanceOf[PucketInstantiator[T]])
      descriptor <- \/.fromTryCatchNonFatal(conf.get(pucketDescriptorKey))
      pucket <- instantiator.newInstance[T](path, fs, descriptor)
    } yield pucket

  /** Get a writer for the task
   *
   * @param pucket the pucket to be written to
   * @return a standard writer or partitoned writer if a partitioner
    *         is present in the descriptor, or an error if validation fails
   */
  def getWriter(pucket: Pucket[T]): Throwable \/ Writer[T, Throwable] =
    pucket.descriptor.partitioner.fold(pucket.writer)(_ => PartitionedWriter[T](pucket).right)

  /** @inheritdoc */
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, T] = {
    val conf = getConfiguration(context)
    conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 2)
    val jobPath = FileOutputFormat.getOutputPath(context)
    val pucket = getPucket(conf, jobPath, FileSystem.get(conf))
    val taskPucket = pucket.flatMap(_.subPucket(FileOutputCommitter.getTaskAttemptPath(context, jobPath)))

    PucketRecordWriter(taskPucket.flatMap(getWriter).throwException)
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter =
    new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context)

}

object PucketOutputFormat {
  val pucketDescriptorKey = "pucket.descriptor"
  val pucketInstantiatorKey = "pucket.instantiator"

  /** Set the pucket descriptor for the output
    *  target in the job configuration
    *
    * @param conf hadoop configuration instance
    * @param descriptor pucket descriptor instance
    * @tparam T type of data to be written
    */
  def setDescriptor[T](conf: Configuration, descriptor: PucketDescriptor[T]): Unit = {
    conf.set(pucketInstantiatorKey, descriptor.instantiatorClass.getName)
    conf.set(pucketDescriptorKey, descriptor.toString)
  }

}


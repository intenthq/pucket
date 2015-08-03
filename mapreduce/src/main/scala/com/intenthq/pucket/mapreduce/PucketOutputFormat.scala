package com.intenthq.pucket.mapreduce

import com.intenthq.pucket._
import com.intenthq.pucket.javacompat.PucketInstantiator
import com.intenthq.pucket.util.ExceptionUtil._
import com.intenthq.pucket.util.PucketPartitioner
import com.intenthq.pucket.writer.{PartitionedWriter, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.parquet.hadoop.util.ConfigurationUtil
import org.apache.parquet.hadoop.util.ContextUtil._

import scalaz.\/
import scalaz.syntax.either._

class PucketOutputFormat[T] extends FileOutputFormat[Void, T] {
  import PucketOutputFormat._

  case class PucketRecordWriter(writer: Writer[T, Throwable]) extends RecordWriter[Void, T] {
    var mutableWriter = writer

    override def write(key: Void, value: T): Unit =
      mutableWriter = mutableWriter.write(value).throwException

    override def close(context: TaskAttemptContext): Unit =
      mutableWriter.close.throwException
  }

  def getPucket(conf: Configuration, path: Path, fs: FileSystem): Throwable \/ Pucket[T] =
    for {
      instantiator <- \/.fromTryCatchNonFatal(
        ConfigurationUtil.
          getClassFromConfig(conf, pucketInstantiatorKey, classOf[PucketInstantiator[T]]).
          newInstance().
          asInstanceOf[PucketInstantiator[T]])
      descriptor <- \/.fromTryCatchNonFatal(conf.get(pucketDescriptorKey))
      pucket <- \/.fromTryCatchNonFatal(instantiator.newInstance[T](path, fs, descriptor))
    } yield pucket

  def getWriter(pucket: Pucket[T]): Throwable \/ Writer[T, Throwable] =
    pucket.descriptor.partitioner.fold(pucket.writer)(_ => PartitionedWriter[T](pucket).right)

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


  def setDescriptor[T](conf: Configuration, descriptor: PucketDescriptor[T]): Unit = {
    conf.set(pucketInstantiatorKey, descriptor.instantiatorClass.getName)
    conf.set(pucketDescriptorKey, descriptor.toString)
  }

}


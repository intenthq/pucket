package com.intenthq.pucket.mapreduce

import java.io.File

import com.intenthq.pucket._
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.avro.{AvroPucket, AvroPucketDescriptor}
import com.intenthq.pucket.javacompat.PucketInstantiator
import com.intenthq.pucket.util.PucketPartitioner
import com.intenthq.pucket.util.ExceptionUtil._
import com.intenthq.pucket.writer.{PartitionedWriter, Writer}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.log4j.{ConsoleAppender, Level, Logger, PatternLayout}
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
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

object FU {


  object ModPatitioner extends PucketPartitioner[AvroTest] {

    override def partition(data: AvroTest, pucket: Pucket[AvroTest]): Throwable \/ Pucket[AvroTest] =
      pucket.subPucket(new Path((data.getTest % 2).toString))
  }


  def main(args: Array[String]): Unit =
  {


    val console = new ConsoleAppender(); //create appender
    //configure the appender
    val PATTERN = "%d [%p|%c|%C{1}] %m%n"
    console.setLayout(new PatternLayout(PATTERN))
    console.setThreshold(Level.DEBUG)
    console.activateOptions()
    //add appender to any Logger (here is root)
    Logger.getRootLogger.addAppender(console)

    val dir = new File("/tmp/puckettest")
    FileUtils.deleteDirectory(dir)
    FileUtils.forceMkdir(dir)
    val conf = new Configuration()


    val outputPath = new Path(dir.getAbsolutePath + "/cunt")
    val inputPath = dir.getAbsolutePath + "/shit"

    FileUtils.forceMkdir(new File(inputPath))


    val descriptor = AvroPucketDescriptor[AvroTest](AvroTest.getClassSchema ,CompressionCodecName.SNAPPY, Some(ModPatitioner))

    val input = AvroPucket.create[AvroTest](new Path(inputPath), FileSystem.get(conf), descriptor)

    input.flatMap(_.writer).flatMap( writer =>
      5.to(11).map(x => new AvroTest(x.toLong)).foldLeft(writer.right[Throwable])( (acc, v) =>
        acc.fold(_.left[Writer[AvroTest, Throwable]], x => x.write(v))
      )
    ).flatMap(_.close)

   // val pucket = AvroPucket[AvroTest](outputPath, FileSystem.get(conf), descriptor)
    PucketOutputFormat.setDescriptor(conf, descriptor)

    val writeJob = Job.getInstance(conf, "write")

    writeJob.setInputFormatClass(classOf[ParquetInputFormat[AvroTest]])
    FileInputFormat.setInputPaths(writeJob, inputPath)
    ParquetInputFormat.setReadSupportClass(writeJob, classOf[AvroReadSupport[AvroTest]])


    writeJob.setOutputFormatClass(classOf[PucketOutputFormat[AvroTest]])

    writeJob.setOutputKeyClass(classOf[Void])
    writeJob.setOutputValueClass(classOf[AvroTest])

    FileOutputFormat.setOutputPath(writeJob, outputPath)
    

    writeJob.setNumReduceTasks(0)
    //writeJob.setMapperClass(classOf[DerpMapper])


    //println(writeJob.toString)

   writeJob.submit()



    while (!writeJob.isComplete) {
      Thread.sleep(1000)
    }
    //of.checkOutputSpecs(job)

    println(writeJob.isSuccessful)
  }
}
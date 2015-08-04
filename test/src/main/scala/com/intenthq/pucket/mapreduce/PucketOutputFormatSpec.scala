package com.intenthq.pucket.mapreduce

import java.util.UUID

import com.intenthq.pucket.{TestLogging, Pucket, PucketDescriptor}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.ParquetInputFormat
import org.scalacheck.{Gen, Prop}
import org.specs2.matcher.DisjunctionMatchers
import org.specs2.{ScalaCheck, Specification}

import scalaz.\/
import scalaz.syntax.either._

trait PucketOutputFormatSpec[T, Descriptor <: PucketDescriptor[T]] extends Specification
                                                                           with ScalaCheck
                                                                           with DisjunctionMatchers
                                                                           with TestLogging {
  import com.intenthq.pucket.TestUtils._

  val pucket: Throwable \/ Pucket[T]
  def descriptorGen: Gen[Descriptor]
  def newData(i: Long): T
  def findPucket(path: Path): Throwable \/ Pucket[T]
  def readSupport: Throwable \/ Class[_]
  def writeClass: Class[T]

  def is =
    s2"""
         Writes data to the pucket ${step(write)}

         Can use the hadoop output format to copy the pucket to different directories with different descriptors $testAll
        Cleans up test directory ${step(FileUtils.deleteDirectory(dir))}
      """


  val data: Seq[T] = 0.to(10).map(x => newData(x.toLong))

  val dir = mkdir

  def write = pucket.flatMap(_.writer).flatMap(writeData(data, _)).flatMap(_.close)

  def testAll = Prop.forAll(descriptorGen) { d =>
    val outputPath = new Path(s"${dir.getAbsolutePath}/${UUID.randomUUID().toString}")
    runJob(outputPath, d) must be_\/- and verify(outputPath)
  }

  def verify(path: Path) =
    findPucket(path) must be_\/-.like {
      case p => readData(data, p) must be_\/-.like {
        case a => a must containAllOf(data)
      }
    }
  
  def runJob(outputPath: Path, descriptor: Descriptor): Throwable \/ Unit =
    readSupport.flatMap( rs => pucket.flatMap { p =>
      val job = Job.getInstance()
      job.setInputFormatClass(classOf[ParquetInputFormat[T]])
      FileInputFormat.setInputPaths(job, path(dir))
      ParquetInputFormat.setReadSupportClass(job, rs)
      FileOutputFormat.setOutputPath(job, outputPath)
      job.setOutputFormatClass(classOf[PucketOutputFormat[T]])

      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(writeClass)
      PucketOutputFormat.setDescriptor(job.getConfiguration, descriptor)


      job.setNumReduceTasks(0)
      \/.fromTryCatchNonFatal {
        job.submit()
        while (!job.isComplete) {
          Thread.sleep(1000)
        }
      }.flatMap( _ =>
          if (job.isSuccessful) ().right
          else new RuntimeException("Job failed!").left
        )
    })
}

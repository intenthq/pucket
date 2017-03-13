package com.intenthq.pucket.writer

import com.intenthq.pucket.{TestLogging, PucketDescriptor}
import com.intenthq.pucket.TestUtils.PucketWrapper
import org.apache.hadoop.fs.Path
import org.specs2.Specification
import com.intenthq.pucket.TestUtils._
import org.specs2.matcher.DisjunctionMatchers

import scalaz.\/

trait IncrementalPartitionedWriterSpec[T] extends Specification with DisjunctionMatchers with TestLogging {
  import IncrementalPartitionedWriterSpec._

  val wrapper: PucketWrapper[T]
  def newData(i: Long): T

  val data = partitions.map(x => newData(x))

  def is =
    s2"""
        Creates subdirectories according to partitioning scheme $writeAndVerify
        Removes the temp dir ${step(wrapper.close())}
     """

  def writeAndVerify = write and verifyDirs

  def write =
    writer.flatMap(writeData(data, _)) must be_\/-.like {
      case a => a.close must be_\/-
    }

  def verifyDirs =
    dirs must be_\/-.like {
      case a =>
        (a.map(_.getName) must containAllOf(partitions.map(_ % 20).distinct.map(_.toString))) and
        (a.filter(_.getName.contains(".parquet")) must beEmpty) and
        (a.map(x => (x.getName, fs.listStatus(x).size)).filter(_ == 3) must beEmpty)
    }


  def dirs: Throwable \/ Seq[Path] =
    wrapper.pucket.flatMap(p => wrapper.pucket.map(x => p.fs.listStatus(x.path).
      map(_.getPath).
      filterNot(_.getName == PucketDescriptor.descriptorFilename)))

  def writer: Throwable \/ IncrementalPartitionedWriter[T] =
    wrapper.pucket.map(IncrementalPartitionedWriter(_, maxWrites, 5))

}

object IncrementalPartitionedWriterSpec {
  val partitions = 0.to(2500)
  val maxWrites = 50
}

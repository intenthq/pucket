package com.intenthq.pucket.writer

import com.intenthq.pucket.{PucketDescriptor, TestLogging}
import com.intenthq.pucket.TestUtils._
import org.specs2.Specification
import org.specs2.matcher.DisjunctionMatchers

import scalaz.\/

trait PartitionedWriterSpec[T] extends Specification with DisjunctionMatchers with TestLogging {
  import PartitionedWriterSpec._

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
      case a => (a must containAllOf(partitions.map(_ % 20).distinct.map(_.toString))) and
                (a.filter(_.contains(".parquet")) must beEmpty)
    }


  def dirs: Throwable \/ Seq[String] =
    wrapper.pucket.flatMap(p => wrapper.pucket.map(x => p.fs.listStatus(x.path).
      map(_.getPath.getName).
      filterNot(_ == PucketDescriptor.descriptorFilename)))

  def writer: Throwable \/ PartitionedWriter[T] = wrapper.pucket.map(PartitionedWriter(_, 5))
}

object PartitionedWriterSpec {
  val partitions = 0.to(2000)
}

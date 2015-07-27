package com.intenthq.pucket.writer

import com.intenthq.pucket.PucketDescriptor
import com.intenthq.thrift.ThriftTestUtils
import ThriftTestUtils._
import com.intenthq.test.model.Test
import org.specs2.Specification
import org.specs2.matcher.DisjunctionMatchers

import scalaz.\/
import scalaz.syntax.either._

//class PartitionedWriterSpec extends Specification with DisjunctionMatchers {
//  import PartitionedWriterSpec._
//
//  def is =
//    s2"""
//        sdfsdf $writeAndVerify
//        Removes the temp dir ${step(pucketWrapper.close())}
//
//     """
//
//  def writeAndVerify = write and verifyDirs
//
//  def write =
//    writeData must be_\/-.like {
//      case a => a.close must be_\/-
//    }
//
//  def verifyDirs =
//    dirs must be_\/-.like {
//      case a => (a must containAllOf(partitions.map(_ % 20).distinct.map(_.toString))) and
//                (a.filter(_.contains(".parquet")) must beEmpty)
//    }
//}
//
//object PartitionedWriterSpec {
//  val partitions = 0.to(2000)
//  val data = partitions.map(x => new Test(x))
//
//  val pucketWrapper = ThriftPucketWrapper.apply()
//  val writer = pucketWrapper.pucket.map(PartitionedWriter[Test])
//
//  def writeData: Throwable \/ PartitionedWriter[Test] =
//    data.foldLeft(writer)( (acc, d) =>
//      acc.fold(_.left, _.write(d))
//    )
//
//  def dirs: Throwable \/ Seq[String] =
//    pucketWrapper.pucket.flatMap(p => pucketWrapper.pucket.map(x => p.fs.listStatus(x.path).
//      map(_.getPath.getName).
//      filterNot(_ == PucketDescriptor.metadataFilename)))
//
//}


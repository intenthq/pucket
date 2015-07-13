package com.intenthq.pucket

import java.io.File

import com.google.common.io.Files
import com.intenthq.pucket.thrift.{ThriftPucket, Thrift, ThriftPucketDescriptor}
import com.intenthq.pucket.util.Partitioner
import com.intenthq.test.model.Test
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalacheck.Gen
import org.specs2.matcher.MatchResult

import scala.reflect.runtime.universe._
import scalaz.\/
import scalaz.syntax.either._

object TestUtils {

  def descriptorGen: Gen[ThriftPucketDescriptor[Test]] = for {
    compression <- Gen.oneOf(CompressionCodecName.values())
    partitioner <- Gen.oneOf(List(Some(ModPartitioner), Some(PassThroughPartitioner), None))
  } yield ThriftPucketDescriptor[Test](classOf[Test], compression, partitioner)


  object ModPartitioner extends Partitioner[Test] {
    override def partition(data: Test, pucket: Pucket[Test]): Throwable \/ Pucket[Test] =
      pucket.subPucket(new Path((data.getTest % 20).toString))
  }

  object PassThroughPartitioner extends Partitioner[Test] {
    override def partition(data: Test, pucket: Pucket[Test]): \/[Throwable, Pucket[Test]] = pucket.right
  }


  case class PucketWrapper[T](dir: File, path: Path, pucket: Throwable \/ Pucket[T]) {
    def close(): Unit = FileUtils.deleteDirectory(dir)

    def runTest(test: Throwable \/ Pucket[T] => MatchResult[Throwable \/ Pucket[T]]): MatchResult[Throwable \/ Pucket[T]] =
      runTest(pucket, test)

    def runTest(p: Throwable \/ Pucket[T],
             test: Throwable \/ Pucket[T] => MatchResult[Throwable \/ Pucket[T]]): MatchResult[Throwable \/ Pucket[T]] = {
      val result = test(p)
      close()
      result
    }
  }

  object PucketWrapper {
    val fs: FileSystem = FileSystem.get(new Configuration())
    val descriptor = ThriftPucketDescriptor(classOf[Test], CompressionCodecName.SNAPPY, Some(ModPartitioner))

    def mkdir: File = Files.createTempDir()
    def path(dir: File) = fs.makeQualified(new Path(dir.getAbsolutePath + "/data/pucket"))

    def apply(): PucketWrapper[Test] = {
      val dir = mkdir
      apply(dir)
    }

    def apply(dir: File): PucketWrapper[Test] =
      apply(dir, path(dir), ThriftPucket.create(path(dir), fs, descriptor))


    def apply[T <: Thrift](des: ThriftPucketDescriptor[T])(implicit tag: TypeTag[T]): PucketWrapper[T] = {
      val dir = mkdir
      apply(dir, path(dir), ThriftPucket.create(path(dir), fs, des))
    }

    def make(make: (Path, FileSystem, ThriftPucketDescriptor[Test]) => Throwable \/ Pucket[Test]): PucketWrapper[Test] = {
      val dir = mkdir
      apply(dir, path(dir), make(path(dir), fs, descriptor))
    }
  }

}

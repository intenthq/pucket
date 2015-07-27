package com.intenthq

import java.io.{File, FileNotFoundException, IOException}

import com.google.common.io.Files
import com.intenthq.pucket.writer.Writer
import com.intenthq.pucket.{Pucket, PucketDescriptor}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalacheck.{Gen, Prop}
import org.specs2.matcher.{MatchResult, DisjunctionMatchers}
import org.specs2.{ScalaCheck, Specification}

import scalaz.\/
import scalaz.syntax.either._


trait PhucketTestSuite[T] extends Specification with DisjunctionMatchers with ScalaCheck {
  import PhucketTestSuite._
  
  def is = tests // allows implementation specific tests to be run in addition to these
  
  def tests =
    s2"""
        Can create a new instance ${create(createWrapper)}
        Can find an existing instance $testExisting
        Fails to find a non-existent instance $nonExisting
        Fails to create over an existing instance $createTwice
        Can find an existing instance when using findOrCreate $findOrCreateExisting
        Can create a new instance when using findOrCreate ${create(findOrCreatePW)}
        Can write to a partition $partition
        Tests absorbtion cases and fails when descriptor is different $testAbsorb
        Fails when trying to absorb the same pucket $absorbSame
      """
  
  def findExisting(path: Path): Throwable \/ Pucket[T]
  def findOrCreate(path: Path, descriptor: PucketDescriptor[T]): Throwable \/ Pucket[T]

  def createWrapper: PucketWrapper[T]
  def createWrapper(dir: File): PucketWrapper[T]
  def createWrapper(descriptor: PucketDescriptor[T]): PucketWrapper[T]
  def findOrCreatePW: PucketWrapper[T]

  def data: Seq[T]
  def descriptor: PucketDescriptor[T]
  def descriptorGen: Gen[PucketDescriptor[T]]
  
  def create(pucket: PucketWrapper[T]) =
    pucket.runTest(readAndWrite)
  
  def nonExisting =
    findExisting(new Path("/")) must be_-\/.like {
      case a => a must beAnInstanceOf[FileNotFoundException]
    } 
  
  def findOrCreateExisting = {
    val pucket = createWrapper
    pucket.runTest(findOrCreate(pucket.path, descriptor), readAndWrite)
  }  
  
  def partition = {
    def test(p: Throwable \/ Pucket[T]) = p must be_\/-.like {
      case a => readAndWrite(a.partition(data.head))
    }
    createWrapper.runTest(test)
  }  
  
  def testExisting = {
    val pucket = createWrapper
    val newPucket = findExisting(pucket.path)
    pucket.runTest(newPucket, readAndWrite)
  }

   def createTwice = {
    val pucket = createWrapper
    def test(p: Throwable \/ Pucket[T]) = p must be_\/-.like {
      case a => createWrapper(pucket.dir).pucket must be_-\/.like {
        case b => b must beAnInstanceOf[IOException]
      }
    }
    pucket.runTest(test)
  }

  def testAbsorb = Prop.forAll(descriptorGen) { d1 =>
    Prop.forAll(descriptorGen) { d2 =>
      val res = absorb(createWrapper(d1), createWrapper(d2))
      if (d1 == d2) res must be_\/-
      else res must be_-\/
    }
  }

  def absorbSame = {
    val pucket = createWrapper
    absorb(pucket, pucket) must be_-\/
  }

  def absorb(pucket1: PucketWrapper[T], pucket2: PucketWrapper[T]) = {
    readAndWrite(pucket1.pucket)
    readAndWrite(pucket2.pucket)

    val ret = pucket1.pucket.flatMap(x => pucket2.pucket.flatMap(x.absorb))

    pucket1.close()
    pucket2.close()

    ret
  }
  
  def readAndWrite(pucket: Throwable \/ Pucket[T]) =
    pucket must be_\/-[Pucket[T]].like {
      case p =>
        write(p) and read(p)
    }

  def write(pucket: Pucket[T]) =
    pucket.writer must be_\/-.like {
      case a =>
        writeData[T](data, a).flatMap(_.close) must be_\/-
    }

  def read(pucket: Pucket[T]) = {
    val reader = pucket.reader
    val result = \/.fromTryCatchNonFatal(0.to(data.length).map(_ => reader.read())) must be_\/-.like {
      case a => a must containAllOf(data)
    }
    reader.close()
    result
  }  
}

object PhucketTestSuite {
  val rng = scala.util.Random

  def writeData[T](data: Seq[T], writer: Writer[T, Throwable]): Throwable \/ Writer[T, Throwable] =
    data.foldLeft(writer.right[Throwable])( (acc, d) =>
      acc.fold(_.left, _.write(d))
    )

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

  val fs: FileSystem = FileSystem.get(new Configuration())
  def path(dir: File) = fs.makeQualified(new Path(dir.getAbsolutePath + "/data/pucket"))
  def mkdir: File = Files.createTempDir()

}

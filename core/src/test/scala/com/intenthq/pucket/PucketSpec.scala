package com.intenthq.pucket

import java.io.{File, FileNotFoundException, IOException}
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.scalacheck.{Gen, Prop}
import org.specs2.matcher.DisjunctionMatchers
import org.specs2.{ScalaCheck, Specification}

import scalaz.\/

trait PucketSpec[T, Descriptor] extends Specification with DisjunctionMatchers with ScalaCheck with TestLogging {
  import PucketSpec._
  import TestUtils._

  def newData(i: Long): T
  def descriptor: Descriptor
  def descriptorGen: Gen[Descriptor]
  def findPucket(path: Path): Throwable \/ Pucket[T]
  def createPucket(path: Path, descriptor: Descriptor): Throwable \/ Pucket[T]
  def findOrCreate(path: Path, descriptor: Descriptor): Throwable \/ Pucket[T]

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

  val data: Seq[T] = 0.to(10).map(_ => newData(rng.nextLong()))

  def createPucket(path: Path): Throwable \/ Pucket[T] =
    createPucket(path, descriptor)

  def createWrapper: PucketWrapper[T] = {
    val dir = mkdir
    createWrapper(dir)
  }

  def createWrapper(dir: File): PucketWrapper[T] =
    PucketWrapper[T](dir, path(dir), createPucket(path(dir)))

  def createWrapper(descriptor: Descriptor): PucketWrapper[T] = {
    val dir = mkdir
    PucketWrapper[T](dir, path(dir), createPucket(path(dir), descriptor))
  }

  def findOrCreatePW: PucketWrapper[T] = {
    val dir = mkdir
    PucketWrapper[T](dir, path(dir), findOrCreate(path(dir), descriptor))
  }


  def create(pucket: PucketWrapper[T]) =
    pucket.runTest(readAndWrite)

  def nonExisting =
    findPucket(new Path("/")) must be_-\/.like {
      case a => a must beAnInstanceOf[FileNotFoundException]
    }

  def findOrCreateExisting = {
    val pucket = createWrapper
    pucket.runTest(findOrCreate(pucket.path, descriptor), readAndWrite)
  }

  def partition = {
    def test(p: Throwable \/ Pucket[T]) = p must be_\/-.like {
      case a => readAndWrite(a.subPucket(a.partition(data.head)))
    }
    createWrapper.runTest(test)
  }

  def testExisting = {
    val pucket = createWrapper
    val newPucket = findPucket(pucket.path)
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
      val (pucket1, pucket2) = (createWrapper(d1), createWrapper(d2))
      val randomSubpath = s"${UUID.randomUUID().toString}/${UUID.randomUUID().toString}"
      val (res, files) = absorb(pucket1, pucket2, Some(randomSubpath))
      if (d1 == d2) (res must be_\/-) and (files must be_\/-.like {
        // test that moved files contain the subpath
        case a => a.count(_.toString.contains(s"/$randomSubpath/")) === a.size
      })
      else res must be_-\/
    }
  }

  def absorbSame = {
    val pucket = createWrapper
    absorb(pucket, pucket)._1 must be_-\/
  }

  def pucketFiles(pucket1: Throwable \/ Pucket[T], pucket2: Throwable \/Pucket[T]) =
    (for {
      p1 <- pucket1
      p2 <- pucket2
      files1 <- p1.listFiles
      files2 <- p2.listFiles
    } yield (files1, files2)) must be_\/-.like {
      case (a, b) => true
    }


  def absorb(pucket1: PucketWrapper[T], pucket2: PucketWrapper[T], subPath: Option[String] = None) = {
    readAndWrite(pucket2.pucket)

    // simulates pucket partitions when a subpath is set
    subPath.foreach( sp =>
      pucket2.pucket.flatMap(_.listFiles).map(_.foreach(path =>
        fs.rename(path, new Path(new Path(path.getParent, new Path(sp)), path.getName))
      ))
    )

    val ret = pucket1.pucket.flatMap(x => pucket2.pucket.flatMap(x.absorb))
    val files = pucket1.pucket.flatMap(_.listFiles)

    pucket1.close()
    pucket2.close()

    (ret, files)
  }

  def readAndWrite(pucket: Throwable \/ Pucket[T]) =
    pucket must be_\/-[Pucket[T]].like {
      case p =>
        write(p) and read(p)
    }

  def write(pucket: Pucket[T]) =
    pucket.writer must be_\/-.like {
      case a =>
        writeData(data, a).flatMap(_.close) must be_\/-
    }

  def read(pucket: Pucket[T]) =
    readData(data, pucket) must be_\/-.like {
      case a => a must containAllOf(data)
    }
}

object PucketSpec {
  val rng = scala.util.Random
}

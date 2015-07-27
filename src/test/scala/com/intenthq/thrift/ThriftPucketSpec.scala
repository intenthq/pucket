package com.intenthq.thrift

import java.io.{File, FileNotFoundException, IOException}

import com.intenthq.pucket.thrift.ThriftPucket
import com.intenthq.pucket.writer.Writer
import com.intenthq.pucket.{Pucket, TestUtils}
import com.intenthq.test.model.Test
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalacheck.Prop
import org.specs2.matcher.{DisjunctionMatchers, MatchResult}
import org.specs2.{ScalaCheck, Specification}

import scalaz.\/
import scalaz.syntax.either._

class PucketSpec extends Specification with DisjunctionMatchers with ScalaCheck {
  import PucketSpec._
  import TestUtils._

  def is =
    s2"""
        Can create a new instance ${create()}
        Can find an existing instance $testExisting
        Fails to find a non-existent instance $nonExisting
        Fails to create over an existing instance $createTwice
        Can find an existing instance when using findOrCreate $findOrCreateExisting
        Can create a new instance when using findOrCreate ${create(PucketWrapper.make(ThriftPucket.findOrCreate[Test]))}
        Can write to a partition $partition
        Tests absorbtion cases and fails when descriptor is different $testAbsorb
        Fails when trying to absorb the same pucket $absorbSame
      """

  def create(pucket: PucketWrapper[Test] = PucketWrapper.apply()) =
    pucket.runTest(readAndWrite)

  def nonExisting =
    ThriftPucket[Test](new Path("/"), FileSystem.get(new Configuration()), classOf[Test]) must be_-\/.like {
      case a => a must beAnInstanceOf[FileNotFoundException]
    }

  def testAndClean[T](dir: File, thing: T, test: T => MatchResult[Throwable \/ Pucket[Test]]) = {
    val result = test(thing)
    FileUtils.deleteDirectory(dir)
    result
  }

  def findOrCreateExisting = {
    val pucket = PucketWrapper.apply()
    pucket.runTest(ThriftPucket.findOrCreate(pucket.path, PucketWrapper.fs, PucketWrapper.descriptor), readAndWrite)
  }

  def partition = {
    def test(p: Throwable \/ Pucket[Test]) = p must be_\/-.like {
      case a => readAndWrite(a.partition(new Test(rng.nextLong())))
    }
    PucketWrapper.apply().runTest(test)
  }

  def testExisting = {
    val pucket =PucketWrapper.apply()
    val newPucket = ThriftPucket[Test](pucket.path, PucketWrapper.fs, classOf[Test])
    pucket.runTest(newPucket, readAndWrite)
  }


  def createTwice = {
    val pucket = PucketWrapper.apply()
    def test(p: Throwable \/ Pucket[Test]) = p must be_\/-.like {
      case a => PucketWrapper(pucket.dir).pucket must be_-\/.like {
        case b => b must beAnInstanceOf[IOException]
      }
    }
    pucket.runTest(test)
  }

  def testAbsorb = Prop.forAll(descriptorGen) { d1 =>
    Prop.forAll(descriptorGen) { d2 =>
      val res = absorb(PucketWrapper(d1), PucketWrapper(d2))
      if (d1 == d2) res must be_\/-
      else res must be_-\/
    }
  }

  def absorbSame = {
    val pucket = PucketWrapper.apply()
    absorb(pucket, pucket) must be_-\/
  }

  def absorb(pucket1: PucketWrapper[Test] = PucketWrapper.apply(), pucket2: PucketWrapper[Test] = PucketWrapper.apply()) = {
    readAndWrite(pucket1.pucket)
    readAndWrite(pucket2.pucket)

    val ret = pucket1.pucket.flatMap(x => pucket2.pucket.flatMap(x.absorb))

    pucket1.close()
    pucket2.close()

    ret
  }

  def readAndWrite(pucket: Throwable \/ Pucket[Test]) =
    pucket must be_\/-[Pucket[Test]].like {
      case p =>
        write(p) and read(p)
    }

  def write(pucket: Pucket[Test]) =
    pucket.writer must be_\/-.like {
      case a =>
        writeData(a).flatMap(_.close) must be_\/-
    }

  def read(pucket: Pucket[Test]) = {
    val reader = pucket.reader
    val result = \/.fromTryCatchNonFatal(0.to(data.length).map(_ => reader.read())) must be_\/-.like {
      case a => a must containAllOf(data)
    }
    reader.close()
    result
  }
}

object PucketSpec {
  val rng = scala.util.Random
  val data = 0.to(10).map(_ => new Test(rng.nextLong()))

  def writeData(writer: Writer[Test, Throwable]): Throwable \/ Writer[Test, Throwable] =
    data.foldLeft(writer.right[Throwable])( (acc, d) =>
      acc.fold(_.left, _.write(d))
    )
}

package com.intenthq.pucket

import java.util.UUID

import com.intenthq.pucket.writer.Writer
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api.ReadSupport
import org.json4s.JsonAST.JValue

import scalaz.\/
import scalaz.syntax.either._

trait Pucket[T] {
  import Pucket._

  def path: Path
  def fs: FileSystem
  def descriptor: PucketDescriptor[T]

  def defaultBlockSize = 50 * 1024 * 1024
  def newInstance(newPath: Path): Pucket[T]
  def readSupportClass: Class[_ <: ReadSupport[T]]
  
  def writer: Throwable \/ Writer[T, Throwable]
  def reader: ParquetReader[T] = reader(None)
  def reader(filter: Option[Filter]): ParquetReader[T]

  val conf = fs.getConf

  def partition(data: T): Throwable \/ Pucket[T] =
    descriptor.partitioner.map(_.partition(data, this)).getOrElse(this.right)

  def subPucket(subPath: Path): Throwable \/ Pucket[T] = {
    val newPath = new Path(path, subPath)
    \/.fromTryCatchNonFatal(fs.mkdirs(newPath)).map(_ => newInstance(newPath))
  }

  def listFiles: Throwable \/ List[Path] =
    \/.fromTryCatchNonFatal(fs.listStatus(path).
                              map(x => fs.makeQualified(x.getPath)).
                              filter(_.getName.endsWith(extension)).
                              toList)

  def absorb(pucket: Pucket[T]): Throwable \/ Unit = {
    def pucketCheck(test: Boolean, message: String): Throwable \/ Unit =
      if (test) ().right
      else new RuntimeException(s"Pucket on path ${pucket.path.toString} " +
                                s"$message pucket on path ${path.toString}").left
    for {
      _ <- pucketCheck(!pucket.path.equals(path),
                        "is on the same path as")
      _ <- pucketCheck(pucket.descriptor.partitioner.equals(descriptor.partitioner),
                        "uses a different partitioning scheme to")
      _ <- pucketCheck(pucket.descriptor.compression.equals(descriptor.compression),
                        "uses a different compression codec to")
      files <- listFiles
      otherFiles <- pucket.listFiles
      _ <- \/.fromTryCatchNonFatal(otherFiles.map(x => (x, filename)).foreach(x => fs.rename(x._1, x._2)))
    } yield ()
  }

  def filename: Path = fs.makeQualified(new Path(path, new Path(s"${UUID.randomUUID().toString}.$extension")))

  def id: String = s"${path.toString}_${descriptor.toString}"
}

trait PucketCompanion {
  type HigherType
  type V
  type DescriptorType[T <: HigherType]

  def apply[T <: HigherType](path: Path, fs: FileSystem, other: V): Throwable \/ Pucket[T]

  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    descriptor: DescriptorType[T]): Throwable \/ Pucket[T]

  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              descriptor: DescriptorType[T]): Throwable \/ Pucket[T]
}

object Pucket {
  val extension = "parquet"

  def compareDescriptors(found: JValue, expected: JValue): Throwable \/ Unit =
    if (found.equals(expected)) ().right
    else new RuntimeException("Found metadata which does not match expected. " +
                              s"Expected: $expected, Found $found").left
  def writeMeta[T](path: Path,
                   fs: FileSystem,
                   descriptor: PucketDescriptor[T]): Throwable \/ Unit =
    for {
      dir <- \/.fromTryCatchNonFatal(fs.mkdirs(path))
      output <- \/.fromTryCatchNonFatal(fs.create(PucketDescriptor.metadataPath(path), false))
      _ <- \/.fromTryCatchNonFatal(output.write(descriptor.toString.getBytes))
      _ <- \/.fromTryCatchNonFatal(output.close())
    } yield ()

  def readMeta(path: Path, fs: FileSystem): Throwable \/ String =
    for {
      input <- \/.fromTryCatchNonFatal(fs.open(PucketDescriptor.metadataPath(path)))
      metadata <- \/.fromTryCatchNonFatal(IOUtils.toString(input))
      _ <- \/.fromTryCatchNonFatal(input.close())
    } yield metadata
}


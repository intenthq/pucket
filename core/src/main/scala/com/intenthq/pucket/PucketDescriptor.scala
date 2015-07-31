package com.intenthq.pucket

import com.intenthq.pucket.util.PucketPartitioner
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scala.reflect.runtime.universe
import scalaz.\/
import scalaz.syntax.either._

trait PucketDescriptor[T] {
  import PucketDescriptor._
  def compression: CompressionCodecName
  def partitioner: Option[PucketPartitioner[T]] = None

  def json: JValue

  def instantiatorClass: Class[_]

  def commonJson = (compressionKey -> compression.name()) ~
                   (partitionerKey -> partitioner.map(_.getClass.getName))

  override def toString: String = compact(render(json))
}

trait PucketDescriptorCompanion {
  type HigherType
  type V

  def apply[T <: HigherType](value: V, descriptorString: String): Throwable \/ PucketDescriptor[T]
  def apply[T <: HigherType](descriptorString: String): Throwable \/ PucketDescriptor[T]
}

object PucketDescriptor {
  val compressionKey = "compression"
  val partitionerKey = "partitioner"

  val metadataFilename = ".pucket.meta"

  implicit val formats = DefaultFormats

  def metadataPath(path: Path): Path = new Path(path, new Path(metadataFilename))

  val rm = universe.runtimeMirror(getClass.getClassLoader)

  def parseDescriptor[T](descriptorString: String): Throwable \/ (Map[String, String], CompressionCodecName, Option[PucketPartitioner[T]]) =
    for {
      descriptorMap <- \/.fromTryCatchNonFatal(parse(descriptorString).extract[Map[String, String]])
      compression <- extractValue(descriptorMap, compressionKey)
      compressionCodec <- \/.fromTryCatchNonFatal(CompressionCodecName.valueOf(compression))
      partitioner <- descriptorMap.get(partitionerKey).
        map(instantiatePartitioner[T]).
        fold[Throwable \/ Option[PucketPartitioner[T]]](None.right[Throwable])(_.map(Option(_)))
    } yield (descriptorMap, compressionCodec, partitioner)

  def instantiatePartitioner[T](className: String): Throwable \/ PucketPartitioner[T] =
    \/.fromTryCatchNonFatal(rm.reflectModule(rm.staticModule(className)).instance.asInstanceOf[PucketPartitioner[T]])

  def extractValue(descriptorMap: Map[String, String], key: String): Throwable \/ String =
    descriptorMap.get(key).
      fold[Throwable \/ String](new RuntimeException(s"Could not find $key in descriptor").left)(_.right)
}

package com.intenthq.pucket

import com.intenthq.pucket.util.Partitioner
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
  def partitioner: Option[Partitioner[T]] = None

  def json: JValue

  def commonJson = (compressionKey -> compression.name()) ~
                   (partitionerKey -> partitioner.map(_.getClass.getName))

  override def toString: String = compact(render(json))
}

object PucketDescriptor {
  val compressionKey = "compression"
  val partitionerKey = "partitioner"

  val metadataFilename = ".pucket.meta"

  implicit val formats = DefaultFormats

  def metadataPath(path: Path): Path = new Path(path, new Path(metadataFilename))

  val rm = universe.runtimeMirror(getClass.getClassLoader)

  def parseDescriptor[T](descriptorString: String): Throwable \/ (Map[String, String], CompressionCodecName, Option[Partitioner[T]]) =
    for {
      descriptorMap <- \/.fromTryCatchNonFatal(parse(descriptorString).extract[Map[String, String]])
      compression <- extractValue(descriptorMap, compressionKey)
      compressionCodec <- \/.fromTryCatchNonFatal(CompressionCodecName.valueOf(compression))
      partitioner <- descriptorMap.get(partitionerKey).
        map(instantiatePartitioner[T]).
        fold[Throwable \/ Option[Partitioner[T]]](None.right[Throwable])(_.map(Option(_)))
    } yield (descriptorMap, compressionCodec, partitioner)

  def instantiatePartitioner[T](className: String): Throwable \/ Partitioner[T] =
    \/.fromTryCatchNonFatal(rm.reflectModule(rm.staticModule(className)).instance.asInstanceOf[Partitioner[T]])

  def extractValue(descriptorMap: Map[String, String], key: String): Throwable \/ String =
    descriptorMap.get(key).
      fold[Throwable \/ String](new RuntimeException(s"Could not find $key in descriptor").left)(_.right)
}

package com.intenthq.pucket

import com.intenthq.pucket.util.PucketPartitioner
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scalaz.\/
import scalaz.syntax.either._

/** Trait for pucket descriptor which gets stored on the filesystem
  * along side the parquet files
  * 
  * @tparam T the type of data being stored in the pucket
  */
trait PucketDescriptor[T] {
  import PucketDescriptor._
  /** Parquet compression codec */
  def compression: CompressionCodecName
  /** Optional pucket partitioner implementation */
  def partitioner: Option[PucketPartitioner[T]] = None

  /** JSON representation of the descriptor */
  def json: JValue

  /** Reflection instantiation class used by the mapreduce module */
  def instantiatorClass: Class[_]

  /** Parquet read support for use when configuring Mapreduce/Spark Jobs */
  def readSupportClass: Class[_ <: ReadSupport[T]]

  /** common JSON values for all implementing descriptors */
  def commonJson = (compressionKey -> compression.name()) ~
                   (partitionerKey -> partitioner.map(_.getClass.getName))

  /** Serialise the descriptor as a JSON string
   *
   * @return JSON string of descriptor
   */
  override def toString: String = compact(render(json))
}

/** Trait for implementations of the pucket descriptor companion object */
trait PucketDescriptorCompanion {
  type HigherType
  type S

  /** Create a new pucket descriptor from a JSON string
    * and validate it against an existing schema type
    *  
    * @param schema schema instance
    * @param descriptorString JSON string of the pucket descriptor
    * @tparam T the type of the pucket data
    * @return a validation error or a new pucket descriptor instance
    */
  def apply[T <: HigherType](schema: S, descriptorString: String): Throwable \/ PucketDescriptor[T]

  /** Create a new pucket descriptor from a JSON string
    *
    * @param descriptorString JSON string of the pucket descriptor
    * @tparam T the type of the pucket data
    * @return a validation error or a new pucket descriptor instance
    */
  def apply[T <: HigherType](descriptorString: String): Throwable \/ PucketDescriptor[T]
}

/** Utility methods for pucket descriptor companion objects */
object PucketDescriptor {
  val compressionKey = "compression"
  val partitionerKey = "partitioner"

  val descriptorFilename = ".pucket.descriptor"

  implicit val formats = DefaultFormats

  /** Full path to a pucket descriptor
    * 
    * @param path path of the pucket
    * @return a new path to the descriptor
    */
  def descriptorFilePath(path: Path): Path = new Path(path, new Path(descriptorFilename))

  /** Parse common elements of a pucket descriptor
   *
   * @param descriptorString JSON string of the pucket descriptor
   * @tparam T the type of the pucket data
   * @return a validation error or a tuple of a string map of all descriptor elements,
    *        the compression codec and an optional partitioner instance
   */
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
    \/.fromTryCatchNonFatal(Class.forName(className).getField("MODULE$").get(null).asInstanceOf[PucketPartitioner[T]])

  def extractValue(descriptorMap: Map[String, String], key: String): Throwable \/ String =
    descriptorMap.get(key).
      fold[Throwable \/ String](new RuntimeException(s"Could not find $key in descriptor").left)(_.right)
}

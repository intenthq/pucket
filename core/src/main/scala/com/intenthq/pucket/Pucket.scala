package com.intenthq.pucket

import scala.language.higherKinds
import java.util.UUID

import com.intenthq.pucket.Pucket.{compareDescriptors, defaultBlockSize}
import com.intenthq.pucket.reader.Reader
import com.intenthq.pucket.util.{HadoopUtil, PucketPartitioner}
import com.intenthq.pucket.writer.Writer
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, Path}
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException
import org.apache.hadoop.ipc.RemoteException
import org.apache.parquet.filter2.compat.FilterCompat.Filter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scalaz.\/
import scalaz.syntax.either._

/** Trait for describing a bucket of data on a filesystem
  *
  *
  * @tparam T The data type the pucket will contain
 */
trait Pucket[T] {
  import Pucket._

  /** Fully qualified hadoop path to the pucket */
  def path: Path
  /** Hadoop filesystem instance */
  def fs: FileSystem
  /** Hadoop configuration */
  val conf = fs.getConf
  /** Descriptor object with information about the pucket. See [[com.intenthq.pucket.PucketDescriptor]] */
  def descriptor: PucketDescriptor[T]

  /** Default Parquet block size */
  def blockSize = 50 * 1024 * 1024

  /** Used when obtaining a new pucket as a subdir of an existing one */
  protected def newInstance(newPath: Path): Pucket[T]

  /** Writer implementation for the pucket. See [[com.intenthq.pucket.writer.Writer]] */
  def writer: Throwable \/ Writer[T, Throwable]

  /** Reader for the pucket. See [[com.intenthq.pucket.reader.Reader]] */
  def reader: Throwable \/ Reader[T] = reader(None)

  /** Reader for the pucket. See [[com.intenthq.pucket.reader.Reader]]
    *
    * @param filter
    * @return a new Reader for reading data from the pucket
    */
  def reader(filter: Option[Filter]): Throwable \/ Reader[T]


  /** Use the partitioner specified in the descriptor to get a new pucket
    *  on a calculated path
    *
    * @param data the object to inspect for partitioning
    * @return a new path which is a subpath under the pucket,
    *         otherwise the current path
    */
  def partition(data: T): Path =
    descriptor.partitioner.map(_.partition(data)).getOrElse(new Path("."))

  /** Create a directory as a subpath of the pucket and
    * create a new pucket instance at the new path
    *
    * @param subPath
    * @return a new pucket at the sub path specified or an error if
    *         the path creation fails
    */
  def subPucket(subPath: Path): Throwable \/ Pucket[T] = {
    val newPath = new Path(path, subPath)
    \/.fromTryCatchNonFatal(fs.mkdirs(newPath)).map(_ => newInstance(newPath))
  }

  /** List all the parquet files in the pucket
    *
    * @return a list of fully qualified paths to parquet files
    */
  def listFiles: Throwable \/ Seq[Path] =
    HadoopUtil.listFiles(path, fs, extension)

  /** Absorb another pucket of the same type into this one
    *
    * Will only absorb if the pucket it is absorbing is on a different path
    * and the descriptor is the same
    *
    * @param pucket the pucket to be absorbed
    */
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
      _ <- \/.fromTryCatchNonFatal(otherFiles.map(x => (x, targetFile(pucket, x))).foreach {x =>
        fs.mkdirs(x._2.getParent)
        fs.rename(x._1, x._2)
      })
    } yield ()
  }

  /** Generate a fully qualified filename under the pucket
    *
    * @return a fully qualified hadoop path to a new randomly generated filename under pucket
    */
  def filename: Path = fs.makeQualified(new Path(path, file))

  /** Generate a filename
    *
    * @return a relative hadoop path to a new randomly generated filename
    */
  def file: Path = new Path(s"${UUID.randomUUID().toString}$extension")


  /** Target location for a given path under the current pucket
    *
    * @param pucket
    * @param otherPath
    * @return a fully qualified path under the current pucket, maintaining a partitioning scheme
    */
  def targetFile(pucket: Pucket[T], otherPath: Path): Path = {
    val relative = new Path(
      StringUtils.removePattern(
        StringUtils.remove(otherPath.toString, fs.makeQualified(pucket.path).toString),
        s"^/"
      )
    )

    new Path(path, new Path(relative.getParent, file))
  }

  /** An identifier for the pucket for use in writer cache
    *
    * @return a string identifier
    */
  val id: Int = this.hashCode()
}

/** Trait for using with Pucket implementations' companion objects
 *  describes functions for creating, or finding puckets on a filesystem
 */
trait PucketCompanion {
  type HigherType
  type V
  type DescriptorType[T <: HigherType] <: PucketDescriptor[T]

  /** Find an existing pucket on the filesystem
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param other the implementing object's parameter for verifying
    *              the found pucket matches expect format
    * @param blockSize the size of a parquet block before memory is flushed
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the pucket
    */
  def apply[T <: HigherType](path: Path, fs: FileSystem, other: V, blockSize: Int): Throwable \/ Pucket[T]

  /** Returns a descriptor of the correct type */
  def getDescriptor[T <: HigherType](schemaSpec: V,
                       compression: CompressionCodecName,
                       partitioner: Option[PucketPartitioner[T]]): DescriptorType[T]


  def getDescriptorSchemaSpec[T <: HigherType](descriptor: DescriptorType[T]): V

  /** Find an existing pucket or create one if it does not exist
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param descriptor a descriptor to create a new pucket with or
    *                  validate an existing pucket against
    * @param blockSize the size of a parquet block before memory is flushed
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the pucket
    */
  def findOrCreateRetry[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    descriptor: DescriptorType[T],
                                    blockSize: Int = defaultBlockSize,
                                    attempts: Int = Pucket.defaultCreationAttempts,
                                    retryIntervalMs: Int = Pucket.defaultRetryIntervalMs): Throwable \/ Pucket[T] = {
    def tryCreate(retries: Int): \/[Throwable, Pucket[T]] = {
      val p = apply[T](path, fs, getDescriptorSchemaSpec(descriptor), blockSize).
          fold(_ => create[T](path, fs, descriptor, blockSize),
                    _.right)
      p.fold(t => {
        t match {
          case e @ (_: FileAlreadyExistsException | _: AlreadyBeingCreatedException | _: RemoteException)
            if (retries > 0 &&
                (!e.isInstanceOf[RemoteException] ||
                 List(classOf[AlreadyBeingCreatedException].getCanonicalName,
                      classOf[FileAlreadyExistsException].getCanonicalName
                 ).contains(e.asInstanceOf[RemoteException].getClassName))) => {
            Thread.sleep(retryIntervalMs)
            tryCreate(retries - 1)
          }
          case _ => t.left[Pucket[T]]
        }
      }, r =>
        r.right[Throwable]
      )
    }

    for {
      pucket <- tryCreate(attempts-1)
      _ <- compareDescriptors(pucket.descriptor.json, descriptor.json)
    } yield pucket
  }

  /** Find an existing pucket or create one if it does not exist
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param schemaSpec schema for the data type
    * @param compression parquet compression codec to use
    * @param partitioner optional partitioning scheme
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the pucket
    */
  def findOrCreateRetry[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    schemaSpec: V,
                                    compression: CompressionCodecName,
                                    partitioner: Option[PucketPartitioner[T]],
                                    attempts: Int,
                                    retryIntervalMs: Int): Throwable \/ Pucket[T] =
    findOrCreateRetry(path, fs, getDescriptor(schemaSpec, compression, partitioner), defaultBlockSize, attempts, retryIntervalMs)

  def findOrCreate[T <: HigherType](path: Path,
                                    fs: FileSystem,
                                    descriptor: DescriptorType[T],
                                    blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T] =
    findOrCreateRetry(path, fs, descriptor, blockSize, 1, Pucket.defaultRetryIntervalMs)

  def findOrCreate[T <: HigherType](path: Path,
                                         fs: FileSystem,
                                         schemaSpec: V,
                                         compression: CompressionCodecName,
                                         partitioner: Option[PucketPartitioner[T]]): Throwable \/ Pucket[T] =
    findOrCreateRetry(path, fs, schemaSpec, compression, partitioner, 1, Pucket.defaultRetryIntervalMs)

  /** Create a new pucket
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param schemaSpec schema for the data type
    * @param compression parquet compression codec to use
    * @param partitioner optional partitioning scheme
    * @tparam T the expected type of the pucket data
    * @return an error if any of the validation fails or the new pucket
    */
  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              schemaSpec: V,
                              compression: CompressionCodecName,
                              partitioner: Option[PucketPartitioner[T]]): Throwable \/ Pucket[T] =
    create[T](path, fs, getDescriptor(schemaSpec, compression, partitioner))
  /** Create a new pucket
    *
    * @param path the path to the pucket
    * @param fs hadoop filesystem instance
    * @param descriptor a descriptor to create a new pucket with
    * @param blockSize the size of a parquet block before memory is flushed
    * @tparam T the type of the pucket data
    * @return an error if any of the validation fails or the new pucket
    */
  def create[T <: HigherType](path: Path,
                              fs: FileSystem,
                              descriptor: DescriptorType[T],
                              blockSize: Int = defaultBlockSize): Throwable \/ Pucket[T]
}

/** Pucket companion object
 *  Provides functions for use with implementing classes and companion objects
 */
object Pucket {
  val defaultBlockSize = 50 * 1024 * 1024
  val extension = ".parquet"
  val defaultCreationAttempts = 1
  val defaultRetryIntervalMs = 250

  /** Validate two JSON serialised descriptors
   *
   * @param found descriptor found on the filesystem
   * @param expected expected descriptor provided by caller
   * @return Validation error or unit
   */
  def compareDescriptors(found: Map[String, String], expected: Map[String, String]): Throwable \/ Unit =
    if (found.equals(expected)) ().right
    else new RuntimeException("Found metadata which does not match expected. " +
                              s"Expected: $expected, Found $found").left

  /** Write descriptor to filesystem
   *
   * @param path path to pucket
   * @param fs hadoop filesystem instance
   * @param descriptor descriptor instance
   * @tparam T the type of the pucket data
   * @return An error if writing failed or unit
   */
  def writeMeta[T](path: Path,
                   fs: FileSystem,
                   descriptor: PucketDescriptor[T]): Throwable \/ Unit =
   for {
      dir <- \/.fromTryCatchNonFatal(fs.mkdirs(path))
      output <- \/.fromTryCatchNonFatal(fs.create(PucketDescriptor.descriptorFilePath(path), false))
      _ <- \/.fromTryCatchNonFatal(output.write(descriptor.toString.getBytes))
      _ <- \/.fromTryCatchNonFatal(output.close())
    } yield ()

  /** Read descriptor from filesystem
   *
   * @param path path to the pucket
   * @param fs hadoop filesystem instance
   * @return the string contents of the metadata file or an error if reading failed
   */
  def readMeta(path: Path, fs: FileSystem): Throwable \/ String =
    for {
      input <- \/.fromTryCatchNonFatal(fs.open(PucketDescriptor.descriptorFilePath(path)))
      metadata <- \/.fromTryCatchNonFatal(IOUtils.toString(input))
      _ <- \/.fromTryCatchNonFatal(input.close())
    } yield metadata
}

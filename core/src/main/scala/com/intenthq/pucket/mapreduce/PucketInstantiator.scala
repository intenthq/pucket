package com.intenthq.pucket.mapreduce

import com.intenthq.pucket.Pucket
import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz.\/

/** Trait whose implementations allow puckets to be
  * instantiated in a Hadoop output format
  *
  * @tparam HigherType the higher type of the data. Eg thrift/avro
  */
trait PucketInstantiator[HigherType] {

  /** Create a new instance of a pucket at a certain path
   *
   * @param path path to the pucket
   * @param fs hadoop filesystem instance
   * @param descriptor descriptor string to be parsed
   * @tparam T data type
   * @return a new pucket instance or an error
   */
  def newInstance[T <: HigherType](path: Path, fs: FileSystem, descriptor: String,
                                   attempts: Int, retryIntervalMs: Int): Throwable \/ Pucket[T]
}

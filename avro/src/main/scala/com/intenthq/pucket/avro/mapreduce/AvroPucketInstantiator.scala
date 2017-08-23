package com.intenthq.pucket.avro.mapreduce

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.avro.{AvroPucket, AvroPucketDescriptor}
import com.intenthq.pucket.mapreduce.PucketInstantiator
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz.\/

/** Avro pucket instanciator for use with output format */
class AvroPucketInstantiator extends PucketInstantiator[IndexedRecord] {
  /** @inheritdoc */
  def newInstance[T <: IndexedRecord](path: Path, fs: FileSystem, descriptor: String,
                                      attempts: Int = Pucket.defaultCreationAttempts,
                                      retryIntervalMs: Int = Pucket.defaultRetryIntervalMs): Throwable \/ Pucket[T] =
    AvroPucketDescriptor[T](descriptor).flatMap(AvroPucket.findOrCreateRetry[T](path, fs, _, Pucket.defaultBlockSize, attempts, retryIntervalMs))
}

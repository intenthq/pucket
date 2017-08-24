package com.intenthq.pucket.thrift.mapreduce

import com.intenthq.pucket.Pucket
import com.intenthq.pucket.mapreduce.PucketInstantiator
import com.intenthq.pucket.thrift.{Thrift, ThriftPucket, ThriftPucketDescriptor}
import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz.\/

/** Thrift pucket instanciator for use with output format */
class ThriftPucketInstantiator extends PucketInstantiator[Thrift] {
  /** @inheritdoc */
  def newInstance[T <: Thrift](path: Path, fs: FileSystem, descriptor: String,
                               attempts: Int = Pucket.defaultCreationAttempts,
                               retryIntervalMs: Int = Pucket.defaultRetryIntervalMs): Throwable \/ Pucket[T] =
    ThriftPucketDescriptor[T](descriptor).flatMap(ThriftPucket.findOrCreateRetry[T](path, fs, _, Pucket.defaultBlockSize, attempts, retryIntervalMs))
}


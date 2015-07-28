package com.intenthq.pucket.mapreduce

import org.apache.hadoop.mapreduce._

class PucketOutputFormat[T] extends OutputFormat[Void, T] {

  class BucketRecordWriter extends RecordWriter[Void, T] {

    override def write(key: Void, value: T): Unit = ???

    override def close(context: TaskAttemptContext): Unit = ???
  }

  override def checkOutputSpecs(context: JobContext): Unit = ???

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = ???

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, T] = ???
}

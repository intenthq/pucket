package com.intenthq.pucket.spark

import com.google.common.io.Files
import com.intenthq.pucket.avro.AvroPucket
import com.intenthq.pucket.avro.test.AvroTest
import com.intenthq.pucket.writer.Writer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import com.intenthq.pucket.spark.PucketSparkAdapter._

import scalaz.\/
import scalaz.syntax.either._

class PucketSparkAdapterSpec extends LocalSparkSpec("PucketSparkAdapter") {
  import PucketSparkAdapterSpec._

  def is =
    s2"""
        asfsdf ${step(fuck)}
      """

  def fuck = println(pucket.map(_.toRDD.collect().toList))
}

object PucketSparkAdapterSpec {
  val dir = Files.createTempDir()

  val conf = new Configuration()
  val fs = FileSystem.get(conf)


  val pucket = AvroPucket.create[AvroTest](new Path(dir.getAbsolutePath + "/in"), fs, AvroTest.getClassSchema, CompressionCodecName.SNAPPY)

  println(new Path(dir.getAbsolutePath + "/in"))

  pucket.flatMap(_.writer).flatMap( writer =>
                                      5.to(11).map(x => new AvroTest(x.toLong)).foldLeft(writer.right[Throwable])( (acc, v) =>
                                                                                                                     acc.fold(_.left[Writer[AvroTest, Throwable]], x => x.write(v))
                                      )
  ).flatMap(_.close)
}

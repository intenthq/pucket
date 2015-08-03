package com.intenthq.pucket.spark

import com.intenthq.pucket.mapreduce.PucketOutputFormat
import com.intenthq.pucket.{Pucket, PucketDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object PucketSparkAdapter {
  implicit class ToRdd[T](pucket: Pucket[T])(implicit ev0: ClassTag[T]) {
    def toRDD(subPaths: List[String])(implicit sc: SparkContext): RDD[T] = {
      val paths = if (subPaths.isEmpty) pucket.path.toString
        else subPaths.map(new Path(pucket.path, _).toString).mkString(",")
      val job = Job.getInstance(sc.hadoopConfiguration)
      ParquetInputFormat.setReadSupportClass(job, pucket.readSupportClass)
      sc.newAPIHadoopFile(paths,
                          classOf[ParquetInputFormat[T]],
                          classOf[Void],
                          ev0.runtimeClass.asInstanceOf[Class[T]],
                          job.getConfiguration
      ).map(_._2)
    }

    def toRDD(implicit sc: SparkContext): RDD[T] =
      toRDD(List.empty)
  }

  implicit class ToPucket[T](rdd: RDD[T])(implicit ev0: ClassTag[T]) extends Serializable {

    def saveAsPucket(path: String, descriptor: PucketDescriptor[T]): Unit =
      rdd.map((null, _)).saveAsNewAPIHadoopFile(
        path,
        classOf[Void],
        ev0.runtimeClass.asInstanceOf[Class[T]],
        classOf[PucketOutputFormat[T]],
        pucketConf(descriptor)
      )

    private def pucketConf(descriptor: PucketDescriptor[T]): Configuration = {
      val conf = rdd.context.hadoopConfiguration
      PucketOutputFormat.setDescriptor[T](conf, descriptor)
      conf
    }

  }
}


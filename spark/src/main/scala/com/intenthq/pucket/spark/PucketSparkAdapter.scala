package com.intenthq.pucket.spark

import com.intenthq.pucket.mapreduce.PucketOutputFormat
import com.intenthq.pucket.{Pucket, PucketDescriptor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

import scala.reflect.ClassTag
/** Provides classes to extend Spark's RDD so that puckets can be read and written with spark */
object PucketSparkAdapter {
  implicit class ToRdd[T](pucket: Pucket[T])(implicit ev0: ClassTag[T]) {

    /** Convert a pucket instance to an RDD
     *
     * Merges hadoop configuration in the spark context with that in the pucket. Note that any parameters in the provided hadoop configuration will take precedence over the hadoop config in the spark context.
     *
     * @param subPaths paths under the pucket to be included in the RDD
     * @param sc implicit spark context
     * @return an RDD of the pucket's data type
     */
    def toRDD(subPaths: List[String])(implicit sc: SparkContext): RDD[T] = {
      val paths = if (subPaths.isEmpty) pucket.path.toString
        else subPaths.map(new Path(pucket.path, _).toString).mkString(",")
      val job = Job.getInstance(sc.hadoopConfiguration)
      ParquetInputFormat.setReadSupportClass(job, pucket.descriptor.readSupportClass)
      sc.newAPIHadoopFile(paths,
                          classOf[ParquetInputFormat[T]],
                          classOf[Void],
                          ev0.runtimeClass.asInstanceOf[Class[T]],
                          mergeConfig(job.getConfiguration, pucket.conf)
      ).map(_._2)
    }

    /** Convert a pucket instance to an RDD
      *
      * @param sc implicit spark context
      * @return an RDD of the pucket's data type
      */
    def toRDD(implicit sc: SparkContext): RDD[T] =
      toRDD(List.empty)
  }

  implicit class ToPucket[T](rdd: RDD[T])(implicit ev0: ClassTag[T]) extends Serializable {

    /** Save an object as a new pucket
     *
     * @param path path to the new pucket
     * @param descriptor pucket descriptor
     * @param configuration optional hadoop configuration to merge with that of the spark context. Note that any parameters in the provided hadoop configuration will take precedence over the hadoop config in the spark context.
     */
    def saveAsPucket(path: String,
                     descriptor: PucketDescriptor[T],
                     configuration: Option[Configuration] = None): Unit =
      rdd.map((null, _)).saveAsNewAPIHadoopFile(
        path,
        classOf[Void],
        ev0.runtimeClass.asInstanceOf[Class[T]],
        classOf[PucketOutputFormat[T]],
        configuration.fold(pucketConf(descriptor))(conf => mergeConfig(pucketConf(descriptor), conf))
      )

    private def pucketConf(descriptor: PucketDescriptor[T]): Configuration = {
      val conf = new Configuration(rdd.context.hadoopConfiguration)
      PucketOutputFormat.setDescriptor[T](conf, descriptor)
      conf
    }
  }

  private def mergeConfig(one: Configuration, two: Configuration): Configuration = {
    val conf = new Configuration(one)
    two.iterator().foreach(entry => conf.set(entry.getKey, entry.getValue))
    conf
  }
}


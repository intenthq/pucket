package com.intenthq.pucket.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.Specification
import org.specs2.specification.core.Fragments

abstract class LocalSparkSpec(appName: String) extends Specification with Serializable {
  implicit def sparkContext: SparkContext = env.get()

  override def map(fs: => Fragments) = step(env.get()) ^ fs ^ step(env.stop())

  private lazy val env = localSpark
  private object localSpark extends Serializable {
    private lazy val context: SparkContext = new SparkContext(
      new SparkConf().setMaster("local").
        setAppName(appName).
        set("spark.sql.testkey", true.toString).
        set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec").
        set("spark.driver.allowMultipleContexts", true.toString)
    )

    def get(): SparkContext = context

    def stop(): Unit = context.stop()
  }
}

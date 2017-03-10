package com.intenthq.pucket.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.specification.BeforeAfterAll

trait LocalSparkSpec extends BeforeAfterAll with Serializable {
  val registrator: Option[String] = None

  implicit lazy val sparkContext: SparkContext = new SparkContext(sparkConf)
  private lazy val sparkConf: SparkConf = {
    val conf = new SparkConf().
      setAppName(this.getClass.getName).
      setMaster("local")

    registrator.fold(conf)(conf.
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator", _)
    )
  }

  override def beforeAll: Unit = sparkContext
  override def afterAll: Unit = sparkContext.stop()
}

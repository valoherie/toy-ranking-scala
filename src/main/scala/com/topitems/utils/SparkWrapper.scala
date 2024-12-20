package com.topitems.utils

import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

//shared sparkSession config
trait SparkWrapper {

  lazy val spark: SparkSession = SparkWrapper.createSparkSession()

  def sparkContext = spark.sparkContext

  //for test cases
  def sparkParallelize[T:ClassTag](data: Seq[T]) =
    spark.sparkContext.parallelize(data)

}

object SparkWrapper {
  private def createSparkSession(): SparkSession = {
    var builder = SparkSession.builder()
      .appName("ComputeTopItems")
      // Set master in dev/test environments
      .master( "local[*]")

    SparkConfig.getSparkConfigs.foreach { case (key, value) =>
      builder.config(key, value)
    }

    builder.getOrCreate()
  }
}

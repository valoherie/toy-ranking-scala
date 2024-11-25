package com.topitems.utils

import org.apache.spark.sql.SparkSession

//shared sparkSession config
trait SparkContextWrapper {

  lazy val spark: SparkSession = SparkContextWrapper.createSparkSession()

  // Make SparkContext easily accessible
  def sparkContext = spark.sparkContext
}

object SparkContextWrapper {
  private def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("ComputeTopItems")
      // Set master in dev/test environments
      .master( "local[*]")
      .getOrCreate()
  }
}

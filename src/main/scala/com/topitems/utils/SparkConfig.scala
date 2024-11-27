package com.topitems.utils

object SparkConfig {
  def getSparkConfigs: Map[String, String] = Map(
    // Resource config
    "spark.executor.memory" -> "8g",
    "spark.executor.instances" -> "10",
    "spark.driver.memory" -> "4g",

    // Memory mgmt
    "spark.memory.fraction" -> "0.8",
    "spark.memory.storageFraction" -> "0.3",
    "spark.default.parallelism" -> "200",

    // Dynamic alloc
    "spark.dynamicAllocation.enabled" -> "true",
    "spark.dynamicAllocation.minExecutors" -> "5",
    "spark.dynamicAllocation.maxExecutors" -> "20",
    "spark.dynamicAllocation.executorIdleTimeout" -> "180s",

    // Performance tuning
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.skewJoin.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
    "spark.sql.shuffle.partitions" -> "200",

    // Monitoring and Debugging
    "spark.eventLog.enabled" -> "true",
    "spark.eventLog.dir" -> "./resources/logs"
  )
}

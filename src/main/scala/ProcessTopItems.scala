import com.topitems.models.Output
import com.topitems.utils.SkewProcessConfig
import com.topitems.utils.SparkWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class ProcessTopItems(skewConfig: SkewProcessConfig = SkewProcessConfig()) extends SparkWrapper {
  def run(
           parquet1: String,
           parquet2: String,
           outputPath: String,
           topX: Int,
           isSkewHandling: Boolean
         ): Unit = {
    try {

      //read input files using DF
      val inputDF1 = spark.read.parquet(parquet1)
      val inputDF2 = spark.read.parquet(parquet2)

      //convert DFs to RDDs
      val df1RDD = inputDF1.rdd.map(row =>
        (row.getAs[Long]("geographical_location_oid"),
          row.getAs[Long]("video_camera_oid"),
          row.getAs[Long]("detection_oid"),
          row.getAs[String]("item_name"),
          row.getAs[Long]("timestamp_detected")
        ))

      val df2RDD = inputDF2.rdd.map(row =>
        (row.getAs[Long]("geographical_location_oid"),
          row.getAs[String]("geographical_location"),
        ))

      //config and check to see if skewed handling is true and process using RDD
      val results = if (isSkewHandling) {
        processTopItemsRDD(df1RDD, df2RDD, topX)
      } else {
        processSkewedTopItems(df1RDD, df2RDD, topX)
      }

      val topItems = results.map(row => Output(row._1, row._2, row._3))
      spark.createDataFrame(topItems)
        .write
        .mode("overwrite")
        .parquet(outputPath)

    } catch {
      case e: Exception =>
        println("Error processing data: ${e.getMessage}")
        e.printStackTrace()
    }
  }

   def processTopItemsRDD(data1: RDD[(Long, Long, Long, String, Long)], data2: RDD[(Long, String)], topX: Int): RDD[(Long, String, Int)] = {
    //remove duplicates
    val uniqueDetectionRDD = removeDuplicatedDetection(data1)

    //count items by location
    val countItemsByLocation = uniqueDetectionRDD
      .map(record => ((record._1, record._4), 1)) // (_1 is geographical_location_oid, _4 is item_name)
      .reduceByKey(_ + _)
      .map { case ((location, itemName), count) => (location, (itemName, count)) }

    //get top x items
    countItemsByLocation
      .groupByKey()
      .flatMapValues(items =>
        items.toList
          .sortBy(x => (-x._2, x._1))
          .take(topX)
      )
      .map { case (location, (item, count)) =>
        (location, item, count)
      }
  }

   def processSkewedTopItems(data1: RDD[(Long, Long, Long, String, Long)], data2: RDD[(Long, String)], topX: Int): RDD[(Long, String, Int)] = {
     val skewThreshold = skewConfig.skewThreshold
     val saltFactor = skewConfig.saltFactor

     val uniqueDetectionRDD = removeDuplicatedDetection(data1)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val countLocation = uniqueDetectionRDD
      .map(record => (record._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    //find skewed keys
    val skewedKeys = countLocation
      .filter(_._2 > skewThreshold)
      .keys.toSet

    val (skewedData, unskewedData) = uniqueDetectionRDD
      .map(record => (record._1, record._4))
      .partitionBySkewedKeys(skewedKeys)

    val processedUnskewedData = unskewedData

    // Handle skewed data with salting
    val processedSkewedData = if (!skewedData.isEmpty()) {
      skewedData.flatMap { case (location, item) =>
        // Add salt to skewed
        (0 until saltFactor).map { salt => ((location, salt), item) }
      }.groupByKey()
        .flatMap { case ((location, _), item) =>
          item.map(item => (location, item))
        }
    } else {
      uniqueDetectionRDD.sparkContext.emptyRDD[(Long, String)]
    }

    val results = processedUnskewedData.union(processedSkewedData)

    //get top x items
    val finalResults =
      results
      .groupByKey()
      .flatMapValues( record =>
        record
          .toList
          .groupBy(identity)
          .mapValues(_.size)
          .toList
          .sortBy(-_._2)
          .take(topX)
      )
          .map { case (location, (item, count)) =>
            (location, item, count)
          }

    uniqueDetectionRDD.unpersist()
    finalResults
  }

   def removeDuplicatedDetection(data1: RDD[(Long, Long, Long, String, Long)]): RDD[(Long, Long, Long, String, Long)] = {
    data1
      .map(detection => (detection._3, detection))
      .reduceByKey((_, value) => value)
      .values
  }

  private implicit class SkewedPartitioning(data: RDD[(Long, String)]) {
    def partitionBySkewedKeys(skewedKeys: Set[Long]): (RDD[(Long, String)], RDD[(Long, String)]) = {
      val skewedData = data.filter { case (location, _) => skewedKeys.contains(location) }
      val unskewedData = data.filter { case (location, _) => !skewedKeys.contains(location) }
      (skewedData, unskewedData)
    }
  }
}

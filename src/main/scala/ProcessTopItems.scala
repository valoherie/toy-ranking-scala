import com.topitems.models.Output
import com.topitems.utils.SparkContextWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class ProcessTopItems() extends SparkContextWrapper {
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

  private def processTopItemsRDD(data1: RDD[(Long, Long, Long, String, Long)], data2: RDD[(Long, String)], topX: Int): RDD[(Long, String, Int)] = {
    //remove duplicates
    val uniqueDetectionRDD = data1
      .map(detection => (detection._3, detection))
      .reduceByKey((_, value) => value)
      .values

    //count items by location
    val countItemsByLocation = removeDuplicatedDetection(data1)
      .map(record => ((record._1, record._4), 1)) // (_1 is geographical_location_oid, _4 is item_name)
      .reduceByKey(_ + _)
      .map { case ((location, itemName), count) => (location, (itemName, count)) }

    //get top x items
    countItemsByLocation
      .groupByKey()
      .flatMapValues(_.toList.sortBy(-_._2).take(topX)) //sort by count
      .map { case (location, (item, count)) =>
        (location, item, count)
      }
  }

  private def processSkewedTopItems(data1: RDD[(Long, Long, Long, String, Long)], data2: RDD[(Long, String)], topX: Int): RDD[(Long, String, Int)] = {
    //remove duplicates
    val uniqueDetectionRDD = removeDuplicatedDetection(data1)
                             .persist(StorageLevel.MEMORY_AND_DISK)
    //count items by location
    val countLocation = uniqueDetectionRDD
      .map(record => (record._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    //find skewed keys
    val skewedKeys = countLocation
      .filter(_._2 > 5)
      .keys.toSet

    //partioning
    val (skewedData, unskewedData) = countLocation
      .partition(record => skewedKeys.contains(record._1))

    val processedUnskewedData = unskewedData

    // Handle skewed data with salting
    val processedSkewedData = if (!skewedData.isEmpty()) {
      skewedData.flatMap { case (location, item) =>
        // Add salt to skewed keys
        (0 until saltFactor).map { salt =>
          ((location, salt), item)
        }
      }
        .groupByKey()
        .map { case ((location, _), item) =>
          (location, item)
        }
    }  else {
      uniqueDetectionRDD.sparkContext.emptyRDD[(Long, String)]
    }

    val results = processedUnskewedData.union(skewedProcessed)

    //get top x items
    results
      .groupByKey()
      .flatMapValues(_.toList.sortBy(-_._2).take(topX)) //sort by count
      .map { case (location, (item, count)) =>
        (location, item, count)
      }

    uniqueDetectionRDD.unpersist()
  }

  private def removeDuplicatedDetection(data1: RDD[(Long, Long, Long, String, Long)]): RDD[(Long, Long, Long, String, Long)] = {
    data1
      .map(detection => (detection._3, detection))
      .reduceByKey((_, value) => value)
      .values
  }
}

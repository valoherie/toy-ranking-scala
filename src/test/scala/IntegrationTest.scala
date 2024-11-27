import com.topitems.utils.SparkWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths

class IntegrationTest extends AnyFunSuite with SparkWrapper {
  import spark.implicits._
  test("ProcessTopItems should process end-to-end workflow correctly") {
    val processor = new ProcessTopItems()

    val detectionSchema = StructType(Array(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("video_camera_oid", LongType, nullable =false),
      StructField("detection_oid", LongType, nullable =false),
      StructField("item_name", StringType, nullable =false),
      StructField("timestamp_detected", LongType,nullable = false)
    ))

    val locationSchema = StructType(Array(
      StructField("geographical_location_oid", LongType, nullable =false),
      StructField("geographical_location", StringType, nullable =false)
    ))

    val testData1 = (Seq(
      // (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp)
      Row(1L, 100L, 1000L, "Barbie Doll", 1732534845123L),
      Row(1L, 100L, 1000L, "Barbie Doll", 1704067200000L),
      Row(1L, 100L, 1004L, "Labubu", 1704067200000L),
      Row(2L, 101L, 1001L, "Toy Car", 1732534845123L),
    ))

    val testData2 = (Seq(
      // (geographical_location_oid, geographical_location)
      Row (1L, "Utopia A"),
      Row(2L, "Utopia B"),
      Row(3L, "Utopia C"),
      Row(4L, "Utopia D"),
      Row(5L, "Utopia E"),
      Row(6L, "Utopia F")
    ))

    val data1DF = spark.createDataFrame(
      spark.sparkContext.parallelize(testData1)
      , detectionSchema)


    val data2DF = spark.createDataFrame(
      spark.sparkContext.parallelize(testData2),
      locationSchema)

    val projectDir = Paths.get("").toAbsolutePath.toString

    val inputFile1 = s"$projectDir/resources/inputFile1"
    val inputFile2 = s"$projectDir/resources/inputFile2"
    val outputPath = s"$projectDir/resources/inputFile2"

    data1DF.write.mode("overwrite").parquet(inputFile1)
    data2DF.write.mode("overwrite").parquet(inputFile2)

    for (isSkewHandling <- Seq(true, false)) { //test both scenarios
      processor.run(inputFile1, inputFile2, s"$outputPath/$isSkewHandling", 2, isSkewHandling)

      val results = spark.read.parquet(s"$outputPath/$isSkewHandling")
                    .as[(Long, String, Int)]
                    .collect()
                    .sortBy(x => (x._1, -x._3))  // Sort by location and count


      assert(results.length == 3)

      // Verify location 1 results
      val location1Results = results.filter(_._1 == 1L)
      assert(location1Results.length == 2)
      assert(location1Results(0)._2 == "Barbie Doll")
      assert(location1Results(0)._3 == 1)

      assert(location1Results(1)._2 == "Labubu")
      assert(location1Results(1)._3 == 1)

      // Verify location 2 results
      val location2Results = results.filter(_._1 == 2L)
      assert(location2Results.length == 1)
      assert(location2Results(0)._2 == "Toy Car")
    }
  }

}

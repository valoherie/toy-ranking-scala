import com.topitems.utils.SparkWrapper
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

import java.io.File
import java.nio.file.Paths
import java.time.Instant
import scala.util.Random


object GenerateInputFiles extends App with SparkWrapper {
  val projectDir = Paths.get("").toAbsolutePath.toString

  val inputFile1 = s"$projectDir/resources/inputFile1"
  val inputFile2 = s"$projectDir/resources/inputFile2"

  new File(s"$projectDir/resources").mkdirs()
  // (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp)
  def generateRandomData1(records:Int) = {
    val random = new Random(50)
    val items = Array("Labubu","Toy Car", "Popmart", "Barbie Doll", "Tomicar", "Monopoly", "Beyblade", "Playdoh", "Jellycat")

    (1 to records).map({ i =>
      val locationID = random.nextInt(100) + 1
      val cameraID = random.nextInt(500) + 1
      val detection_old_ID = i
      val item = items(random.nextInt((items.length)))
      val timeStamp = Instant.now().getEpochSecond

     Row(locationID.toLong, cameraID.toLong, detection_old_ID, item, timeStamp)
    })

  }

  def generateLocationData(records: Int) = {
    val towns = Array("Utopia A", "Utopia B", "Utopia C","Utopia D",
                       "Utopia E", "Utopia F", "Utopia G","Utopia H", "Utopia I",
                       "Utopia J", "Utopia K")

    (1 to records).map { i =>
      val random = new Random(i)
      val locationID = random.nextInt(i) + 1
      val locationName = towns(random.nextInt((towns.length)))

      Row(locationID.toLong, locationName)
    }
  }

  def generateInputParquetFiles(  outputPath1: String,
                                  outputPath2: String,
                                  data1Recordno: Int = 10000,
                                  data2Recordno: Int = 100): Unit = {

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

    val detectionData = generateRandomData1(data1Recordno)
    val detectionDF = spark.createDataFrame(
                      spark.sparkContext.parallelize(detectionData)
                      , detectionSchema)

    val locationData = generateLocationData(data2Recordno)
    val locationDF = spark.createDataFrame(
      spark.sparkContext.parallelize(locationData),
      locationSchema
    )

    detectionDF
      .repartition(20)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath1)

    locationDF
      .repartition(5)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath2)

  }
  generateInputParquetFiles(inputFile1, inputFile2)
  spark.stop()
  println("Parquet files 1 and 2 created successfully!")

}

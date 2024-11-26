import com.topitems.utils.SparkWrapper
import org.scalatest.funsuite.AnyFunSuite

class ComputeTopItemsTest extends AnyFunSuite with SparkWrapper {

  val process = new ProcessTopItems()


 test("removeDuplicatedDetection should remove duplicate detections based on detection_oid") {
    val testData1 = sparkParallelize(Seq(
      // (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp)
      (1L, 100L, 1000L, "Toy A", 1732534845123L),
      (1L, 100L, 1000L, "Toy A", 1704067200000L),
      (1L, 100L, 1000L, "Toy A", 1732534845123L),
      (1L, 100L, 1001L, "Toy Car", 1732534845123L),
      (1L, 101L, 1002L, "Toy Car", 1704067200000L), // duplicate item at location 1
      (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
      (2L, 102L, 1004L, "Jellycat", 1609459200000L),
      (2L, 103L, 1005L, "Jellycat", 1704067200000L),
      (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (2L, 103L, 1005L, "Jellycat", 1704067200000L),
      (2L, 103L, 1005L, "Jellycat", 1704067200000L),
      (2L, 103L, 1005L, "Jellycat", 1704067200000L),
      (3L, 103L, 1005L, "Labubu", 1704067200000L),
      (4L, 103L, 1005L, "Labubu", 1704067200000L),
      (4L, 103L, 1005L, "Labubu", 1704067200000L),
      (4L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (5L, 103L, 1005L, "Tomi Car", 1704067200000L),
      (6L, 103L, 1005L, "Star Wars", 1704067200000L)
    ))


    val testData2 = sparkParallelize(Seq(
      // (geographical_location_oid, geographical_location)
      (1L, "Utopia A"),
      (2L, "Utopia B"),
      (3L, "Utopia C"),
      (4L, "Utopia D"),
      (5L, "Utopia E"),
      (6L, "Utopia F")
    ))


    // Expected: (location_oid, item_name, rank)
    val expectedResults = Set(
      (1L, 100L, 1000L, "Toy A", 1732534845123L),
      (1L, 100L, 1001L, "Toy Car", 1732534845123L),
      (1L, 101L, 1002L, "Toy Car", 1704067200000L),
      (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
      (2L, 102L, 1004L, "Jellycat", 1609459200000L),
      (2L, 103L, 1005L, "Jellycat", 1704067200000L),
      (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (3L, 103L, 1005L, "Labubu", 1704067200000L),
      (4L, 103L, 1005L, "Labubu", 1704067200000L),
      (4L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (5L, 103L, 1005L, "Tomi Car", 1704067200000L),
      (6L, 103L, 1005L, "Star Wars", 1704067200000L)
    )

   val results = process.removeDuplicatedDetection(testData1).collect()
   assert(results.toSet == expectedResults.toSet)
 }

}

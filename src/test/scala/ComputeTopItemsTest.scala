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
      (1L, 101L, 1002L, "Toy Car", 1704067200000L),
      (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
      (2L, 102L, 1004L, "Jellycat", 1609459200000L),
      (2L, 103L, 1005L, "Jellycat", 1704067200000L),
      (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1007L, "Jellycat", 1704067200000L),
      (3L, 103L, 1008L, "Labubu", 1704067200000L),
      (4L, 103L, 1009L, "Labubu", 1704067200000L),
      (4L, 103L, 1009L, "Labubu", 1704067200000L),
      (4L, 103L, 1010L, "Barbie Doll", 1704067200000L),
      (5L, 103L, 1011L, "Tomi Car", 1704067200000L),
      (6L, 103L, 1011L, "Star Wars", 1704067200000L)
    ))


    // Expected: (location_oid, item_name, rank)
    val expectedResults = Set(
      (1L, 100L, 1000L, "Toy A", 1732534845123L),
      (1L, 100L, 1001L, "Toy Car", 1732534845123L),
      (1L, 101L, 1002L, "Toy Car", 1704067200000L),
      (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
      (2L, 102L, 1004L, "Jellycat", 1609459200000L),
      (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1007L, "Jellycat", 1704067200000L),
      (3L, 103L, 1008L, "Labubu", 1704067200000L),
      (4L, 103L, 1009L, "Labubu", 1704067200000L),
      (4L, 103L, 1010L, "Barbie Doll", 1704067200000L),
      (6L, 103L, 1011L, "Star Wars", 1704067200000L)
    )

   val results = process.removeDuplicatedDetection(testData1).collect()
   assert(results.toSet == expectedResults.toSet)
 }

  test("processTopItemsRDD should return top items per location without skew handling") {
    val testData1 = sparkParallelize(Seq(
      // (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp)
      (1L, 100L, 1000L, "Puzzle", 1732534845123L),
      (1L, 100L, 1100L, "Puzzle", 1704067200000L),
      (1L, 100L, 11100L, "Puzzle", 1732534845123L),
      (1L, 100L, 1001L, "Toy Car", 1732534845123L),
      (1L, 101L, 1002L, "Toy Car", 1704067200000L),
      (1L, 101L, 1023L, "Toy Car", 1704067200001L),
      (1L, 101L, 1103L, "Toy Car", 1704067200001L),
      (1L, 101L, 1038L, "Barbie Doll", 1704067200001L),
      (1L, 101L, 1039L, "Barbie Doll", 1704067200001L),
      (1L, 101L, 1040L, "Barbie Doll", 1704067200001L),
      (1L, 108L, 1041L, "Star Wars", 1732534845123L),
      (1L, 108L, 1042L, "Star Wars", 1732534845123L),
      (1L, 108L, 1043L, "Jellycat", 1732534845123L),

      (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
      (2L, 102L, 1004L, "Jellycat", 1609459200000L),
      (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1007L, "Jellycat", 1704067200000L),
      (2L, 103L, 1015L, "Jellycat", 1704067200000L),
      (2L, 106L, 1020L, "Jellycat", 1704067200000L),
      (2L, 108L, 1021L, "Jellycat", 1732534845123L),
      (2L, 108L, 1025L, "Barbie Doll", 1732534845123L),
      (2L, 108L, 1028L, "Barbie Doll", 1732534845123L),
      (2L, 108L, 1024L, "Star Wars", 1732534845123L),
      (2L, 108L, 1029L, "Labubu", 1732534845123L),
      (2L, 108L, 1030L, "Labubu", 1732534845123L),
      (2L, 108L, 1031L, "Labubu", 1732534845123L),
      (2L, 108L, 1032L, "Labubu", 1732534845123L),
      (2L, 108L, 1033L, "Labubu", 1732534845123L),
      (2L, 108L, 1034L, "Pooh", 1732534845123L),
      (2L, 108L, 1035L, "Pooh", 1732534845123L),
      (2L, 108L, 1036L, "Pooh", 1732534845123L),

      (3L, 103L, 1045L, "Labubu", 1732534845123L),
      (3L, 103L, 1044L, "Labubu", 1732534845123L),
      (3L, 103L, 1046L, "Labubu", 1732534845123L),

      (4L, 103L, 1009L, "Labubu", 1704067201100L),
      (4L, 103L, 10023L, "Labubu", 1704067200000L),
      (4L, 103L, 10018L, "Labubu", 1704067200000L),
      (4L, 103L, 10022L, "Labubu", 1704067200000L),
      (4L, 103L, 1010L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1047L, "Pooh", 1704067200000L),
      (4L, 103L, 1048L, "Pooh", 1704067200000L),
      (4L, 103L, 1049L, "Pooh", 1704067200000L),
      (4L, 103L, 1050L, "Jellycat", 1704067200000L),
      (4L, 103L, 1051L, "Jellycat", 1704067200000L),
      (4L, 103L, 1052L, "Jellycat", 1704067200000L),
      (4L, 103L, 1053L, "Jellycat", 1704067200000L),
      (4L, 103L, 1054L, "Jellycat", 1704067200000L),
      (4L, 103L, 1055L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1056L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1057L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1058L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1059L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1060L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1061L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1062L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1063L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1065L, "Star Wars", 1704067200000L),
      (4L, 103L, 1064L, "Star Wars", 1704067200000L),

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

    val expectedResult = Seq(
      (1L, "Toy Car",4),
      (1L, "Puzzle",3),
      (1L, "Barbie Doll",3),
      (1L, "Star Wars",2),
      (1L, "Jellycat",1),

      (2L, "Jellycat",6),
      (2L, "Barbie Doll",4),
      (2L, "Labubu",5),
      (2L, "Pooh",3),
      (2L, "Star Wars",1),

      (3L, "Labubu",3),

      (4L, "Labubu",4),
      (4L, "Pooh",3),
      (4L, "Barbie Doll",10),
      (4L, "Star Wars",2),
      (4L, "Jellycat",5)
    )

    val result = process.processTopItemsRDD(testData1, testData2, 5).collect()
    assert(result.toSet == expectedResult.toSet)
  }

  test("processSkewedTopItems should return top items per location with skew handling") {
    val testData1 = sparkParallelize(Seq(
      // (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp)
      (1L, 100L, 1000L, "Puzzle", 1732534845123L),
      (1L, 100L, 1100L, "Puzzle", 1704067200000L),
      (1L, 100L, 11100L, "Puzzle", 1732534845123L),
      (1L, 100L, 1001L, "Toy Car", 1732534845123L),
      (1L, 101L, 1002L, "Toy Car", 1704067200000L),
      (1L, 101L, 1023L, "Toy Car", 1704067200001L),
      (1L, 101L, 1103L, "Toy Car", 1704067200001L),
      (1L, 101L, 1038L, "Barbie Doll", 1704067200001L),
      (1L, 101L, 1039L, "Barbie Doll", 1704067200001L),
      (1L, 101L, 1040L, "Barbie Doll", 1704067200001L),
      (1L, 108L, 1041L, "Star Wars", 1732534845123L),
      (1L, 108L, 1042L, "Star Wars", 1732534845123L),
      (1L, 108L, 1043L, "Jellycat", 1732534845123L),

      (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
      (2L, 102L, 1004L, "Jellycat", 1609459200000L),
      (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1006L, "Jellycat", 1704067200000L),
      (2L, 103L, 1007L, "Jellycat", 1704067200000L),
      (2L, 103L, 1015L, "Jellycat", 1704067200000L),
      (2L, 106L, 1020L, "Jellycat", 1704067200000L),
      (2L, 108L, 1021L, "Jellycat", 1732534845123L),
      (2L, 108L, 1025L, "Barbie Doll", 1732534845123L),
      (2L, 108L, 1028L, "Barbie Doll", 1732534845123L),
      (2L, 108L, 1024L, "Star Wars", 1732534845123L),
      (2L, 108L, 1029L, "Labubu", 1732534845123L),
      (2L, 108L, 1030L, "Labubu", 1732534845123L),
      (2L, 108L, 1031L, "Labubu", 1732534845123L),
      (2L, 108L, 1032L, "Labubu", 1732534845123L),
      (2L, 108L, 1033L, "Labubu", 1732534845123L),
      (2L, 108L, 1034L, "Pooh", 1732534845123L),
      (2L, 108L, 1035L, "Pooh", 1732534845123L),
      (2L, 108L, 1036L, "Pooh", 1732534845123L),

      (3L, 103L, 1045L, "Labubu", 1732534845123L),
      (3L, 103L, 1044L, "Labubu", 1732534845123L),
      (3L, 103L, 1046L, "Labubu", 1732534845123L),

      (4L, 103L, 1009L, "Labubu", 1704067201100L),
      (4L, 103L, 10023L, "Labubu", 1704067200000L),
      (4L, 103L, 10018L, "Labubu", 1704067200000L),
      (4L, 103L, 10022L, "Labubu", 1704067200000L),
      (4L, 103L, 1010L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1047L, "Pooh", 1704067200000L),
      (4L, 103L, 1048L, "Pooh", 1704067200000L),
      (4L, 103L, 1049L, "Pooh", 1704067200000L),
      (4L, 103L, 1050L, "Jellycat", 1704067200000L),
      (4L, 103L, 1051L, "Jellycat", 1704067200000L),
      (4L, 103L, 1052L, "Jellycat", 1704067200000L),
      (4L, 103L, 1053L, "Jellycat", 1704067200000L),
      (4L, 103L, 1054L, "Jellycat", 1704067200000L),
      (4L, 103L, 1055L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1056L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1057L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1058L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1059L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1060L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1061L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1062L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1063L, "Barbie Doll", 1704067200000L),
      (4L, 103L, 1065L, "Star Wars", 1704067200000L),
      (4L, 103L, 1064L, "Star Wars", 1704067200000L),

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


    val result = process.processSkewedTopItems(testData1, testData2, 2).collect()
    val location1Items = result.filter(_._1 == 1L)
    val location2Items = result.filter(_._1 == 2L)

    //location 1
    assert(location1Items.length == 2)
    assert(location1Items(0) == (1L, "Toy Car", 4))
    assert(location1Items(1)._1 == 1L)
    assert(location1Items(1)._3 == 3)
    assert(Set("Puzzle", "Barbie Doll").contains(location1Items(1)._2))

    //location 2
    assert(location2Items.length == 2)
    assert(location2Items(0) == (2L, "Jellycat", 6))
    assert(location2Items(1) == (2L, "Labubu", 5))
  }
}

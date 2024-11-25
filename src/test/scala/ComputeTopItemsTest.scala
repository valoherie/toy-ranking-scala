class ComputeTopItemsTest {

  // Test data
  val testData1: Seq[(Long, Long, Long, String, Long)] = Seq(
    // (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp)
    (1L, 100L, 1000L, "Toy A", 1732534845123L ),
    (1L, 100L, 1001L, "Toy Car", 1732534845123L),
    (1L, 101L, 1002L, "Toy Car", 1704067200000L),  // duplicate item at location 1
    (2L, 102L, 1003L, "Barbie Doll", 1704067200000L),
    (2L, 102L, 1004L, "Jellycat", 1609459200000L),
    (2L, 103L, 1005L, "Jellycat", 1704067200000L),
    (2L, 103L, 1005L, "Barbie Doll", 1704067200000L),
    (2L, 103L, 1005L, "Jellycat", 1704067200000L),
    (2L, 103L, 1005L, "Jellycat", 1704067200000L),
    (2L, 103L, 1005L, "Jellycat", 1704067200000L)
  )

  val testData2: Seq[(Long, String)] = Seq(
    // (geographical_location_oid, geographical_location)
    (1L, "Utopia A"),
    (2L, "Utopia B"),
    (3L, "Utopia C"),
    (4L, "Utopia D"),
    (5L, "Utopia E"),
    (6L, "Utopia F")
  )

  "ComputeTopItems" should "correctly count top items by location" in {
    // Convert test data to RDDs
    val data1RDD: RDD[(Long, Long, Long, String, Long)] = spark.sparkContext.parallelize(testData1)
    val data2RDD: RDD[(Long, String)] = spark.sparkContext.parallelize(testData2)

    // Process data
    val results = ComputeTopItems.processTopItemsRDD(data1RDD, data2RDD, 2)

    // Collect results and convert to Set for comparison
    val actualResults = results.collect().toSet

    // Expected results: (location_oid, item_name, rank)
    val expectedResults = Set(

    )

    actualResults should equal(expectedResults)
  }

}

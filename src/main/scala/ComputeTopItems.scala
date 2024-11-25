import org.apache.spark.rdd.RDD

object ComputeTopItems extends App {

    def compute(args: Array[String]): Unit = {
      val inputPath1 = args(0)
      val inputPath2 = args(1)
      val outputPath = args(2)
      val topX = args(3).toInt
      val isSkewHandling = args(4).toBoolean

      val processor = new ProcessTopItems()
      processor.run(inputPath1, inputPath2, outputPath, topX, isSkewHandling)
      }
}

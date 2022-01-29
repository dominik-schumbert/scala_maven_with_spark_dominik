import org.apache.spark.sql.{Dataset, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val logFile = "src/main/resources/test.txt"
    val spark: SparkSession = SparkSession
      .builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()
    val logData: Dataset[String] = spark.read.textFile(logFile).cache()
    val numOs: Long = logData.filter(_.contains("o")).count()
    println(numOs)
    spark.stop()
  }
}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    // Read from a sample text file
    val logFile = "src/main/resources/test.txt"
    val spark: SparkSession = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()

    val logData: Dataset[String] = spark.read.textFile(logFile).cache()
    // Get all O characters
    val numOs: Long = logData.filter(_.contains("o")).count()
    println(numOs)

    val df: DataFrame = spark.read.jdbc(url = "jdbc:mysql://localhost:3306/nordwind",
      table = "artikel", properties = getProperties)
    // Filter DataFrame
    df
      .filter(df("Lieferanten-Nr") === 1)
      .show(truncate = false)

    spark.stop()
  }
  def getProperties: Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", "dominikschumbert")
    connectionProperties.put("password", "")
    return connectionProperties
  }
}

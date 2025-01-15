package jobs.Aggregations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when}
import utils.SparkConfig.sparkSession

class DevicesByTypeAggJob(inputPath: String, outputPath: String) {

  def run(): Unit = {
    try {
      val sensorDf = sparkSession.read.parquet(inputPath)
      val devicesByTypeDf = DevicesByTypeAggJob.computeDevicesByType(sensorDf)

      devicesByTypeDf.write
        .mode("overwrite")
        .parquet(outputPath)

      devicesByTypeDf.show()
    } catch {
      case e: Exception =>
        println(s"Error running DevicesByTypeAggJob: ${e.getMessage}")
        throw e
    }
  }
}

object DevicesByTypeAggJob {
  def computeDevicesByType(sensorDf: DataFrame): DataFrame = {
    sensorDf
      .groupBy("sensorType")
      .agg(
        count("id").as("totalDevices"),
        count(when(col("status") === "active", 1)).as("activeDevices")
      )
      .orderBy("sensorType")
  }
}

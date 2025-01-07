package jobs.Aggregations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when}
import utils.SparkConfig.sparkSession

class DevicesByLocationAggJob(inputPath: String, outputPath: String) {

  def run(): Unit = {
    try {
      val sensorDf = sparkSession.read.parquet(inputPath)
      val devicesByLocationDf = DevicesByLocationAggJob.computeDevicesByLocation(sensorDf)

      devicesByLocationDf.write
        .mode("overwrite")
        .parquet(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error running DevicesByLocationAggJob: ${e.getMessage}")
        throw e
    }
  }
}

object DevicesByLocationAggJob {
  def computeDevicesByLocation(sensorDf: DataFrame): DataFrame = {
    sensorDf
      .groupBy("location")
      .agg(
        count("id").as("totalDevices"),
        count(when(col("status") === "active", 1)).as("activeDevices"),
        count(when(col("connectivity") === "online", 1)).as("onlineDevices")
      )
      .orderBy("location")
  }
}


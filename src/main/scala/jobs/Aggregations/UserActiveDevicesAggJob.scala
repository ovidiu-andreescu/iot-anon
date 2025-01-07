package jobs.Aggregations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}
import utils.SparkConfig.sparkSession

class UserActiveDevicesAggJob(inputPath: String, outputPath: String) {

  def run(): Unit = {
    try {
      val sensorDf = sparkSession.read.parquet(inputPath)
      val activeDevicesDf = UserActiveDevicesAggJob.computeActiveDevices(sensorDf)

      activeDevicesDf.write
        .mode("overwrite")
        .parquet(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error running UserActiveDevicesAggJob: ${e.getMessage}")
        throw e
    }
  }
}

object UserActiveDevicesAggJob {
  def computeActiveDevices(sensorDf: DataFrame): DataFrame = {
    sensorDf
      .filter(col("status") === "active")
      .groupBy("userId")
      .agg(count("id").as("activeDevices"))
      .orderBy("userId")
  }
}

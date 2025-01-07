package jobs.Aggregations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, max, min}
import utils.SparkConfig.sparkSession

class BatteryLevelStatsJob(inputPath: String, outputPath: String) {

  def run(): Unit = {
    try {
      val sensorDf = sparkSession.read.parquet(inputPath)
      val batteryStatsDf = BatteryLevelStatsJob.computeBatteryLevelStats(sensorDf)

      batteryStatsDf.show()

      batteryStatsDf.write
        .mode("overwrite")
        .parquet(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error running BatteryLevelStatsJob: ${e.getMessage}")
        throw e
    }
  }
}

object BatteryLevelStatsJob {
  def computeBatteryLevelStats(sensorDf: DataFrame): DataFrame = {
    sensorDf
      .filter(col("batteryLevel").isNotNull)
      .groupBy("userId")
      .agg(
        avg("batteryLevel").as("avgBatteryLevel"),
        min("batteryLevel").as("minBatteryLevel"),
        max("batteryLevel").as("maxBatteryLevel")
      )
      .orderBy("userId")
  }
}

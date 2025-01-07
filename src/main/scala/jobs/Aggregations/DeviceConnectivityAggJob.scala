package jobs.Aggregations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import utils.SparkConfig.sparkSession

class DeviceConnectivityAggJob(inputPath: String, outputPath: String) {

  def run(): Unit = {
    try {
      val sensorDf = sparkSession.read.parquet(inputPath)
      val connectivityDf = DeviceConnectivityAggJob.computeConnectivityStats(sensorDf)

      connectivityDf.write
        .mode("overwrite")
        .parquet(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error running DeviceConnectivityAggJob: ${e.getMessage}")
        throw e
    }
  }
}

object DeviceConnectivityAggJob {
  def computeConnectivityStats(sensorDf: DataFrame): DataFrame = {
    sensorDf
      .groupBy("connectivity")
      .agg(count("id").as("deviceCount"))
      .orderBy("connectivity")
  }
}

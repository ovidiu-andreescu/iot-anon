package jobs.Aggregations

import io.prometheus.client.{CollectorRegistry, Gauge, Counter}
import io.prometheus.client.exporter.PushGateway
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SparkConfig.sparkSession

class DevicesByLocationAggJob(inputPath: String, outputPath: String) {
  private val registry = new CollectorRegistry()
  private val rowsGauge = Gauge.build()
    .name("devices_by_location_job_rows")
    .help("Number of rows processed by the job")
    .register(registry)
  private val jobRunsCounter = Counter.build()
    .name("devices_by_location_job_runs")
    .help("Number of times the job has been run")
    .register(registry)
  private val jobStatusGauge = Gauge.build()
    .name("devices_by_location_job_status")
    .help("Current status of the job: 1 for running, 0 for not running")
    .register(registry)

  private val pushGateway = new PushGateway("http://localhost:9090") // Replace with your PushGateway URL

  def run(): Unit = {
    jobRunsCounter.inc()
    jobStatusGauge.set(1)

    try {
      val sensorDf = sparkSession.read.parquet(inputPath)
      rowsGauge.set(sensorDf.count())

      val devicesByLocationDf = DevicesByLocationAggJob.computeDevicesByLocation(sensorDf)
      devicesByLocationDf.write
        .mode("overwrite")
        .parquet(outputPath)

      devicesByLocationDf.show()

      pushGateway.pushAdd(registry, "devices_by_location_job")
    } catch {
      case e: Exception =>
        println(s"Error running DevicesByLocationAggJob: ${e.getMessage}")
        throw e
    } finally {
      jobStatusGauge.set(0)
      pushGateway.pushAdd(registry, "devices_by_location_job")
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

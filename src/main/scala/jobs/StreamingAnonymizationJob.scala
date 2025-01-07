package jobs

import jobs.StreamingAnonymizationJob.anonymizeSensorDf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import utils.KafkaConfig
import utils.SparkConfig.sparkSession

import java.nio.file.{Files, Paths}

class StreamingAnonymizationJob(
                                 outputPath: String,
                                 checkpointPath: String
                               ) {

  def run(): Unit = {
    try {
      val kafkaBootstrapServers = KafkaConfig.Kafka.bootstrapServers
      val kafkaTopic            = KafkaConfig.Kafka.topic
      val sensorAvroSchema      = new String(Files.readAllBytes(Paths.get("./src/main/resources/SensorReading.avsc")))

      val rawDf = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServers)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", "latest")
        .load()


      val parsedDf = rawDf
        .select(from_avro(col("value"), sensorAvroSchema).as("sensorData"))
        .select("sensorData.*")

      val anonymizedDf = anonymizeSensorDf(parsedDf)

      val query = anonymizedDf.writeStream
        .format("parquet")
        .option("path", outputPath)
        .option("checkpointLocation", checkpointPath)
        .outputMode(OutputMode.Append)
        .start()

      query.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"Error running BatteryLevelStatsJob: ${e.getMessage}")
        throw e
    }
  }
}

object StreamingAnonymizationJob
{
  val maskStringUdf: UserDefinedFunction = udf { (str: String) =>
    if (str == null) null
    else {
      val parts = str.split(" ", 2)
      if (parts.length < 2) "xxxx"
      else "xxxx " + parts(1)
    }
  }

  val addNoiseDoubleUdf: UserDefinedFunction = udf { (value: Double) =>
    val noise = (Math.random() - 0.5)
    value + noise
  }

  val coarsenTimestampUdf: UserDefinedFunction = udf { (ts: Long) =>
    val hourMs = 3600000L
    (ts / hourMs) * hourMs
  }

  def anonymizeSensorDf(sensorDf: DataFrame): DataFrame = {
    sensorDf
      .withColumn("id", sha2(col("id"), 256))
      .withColumn("serialNumber", sha2(col("serialNumber"), 256))
      .withColumn("userId", sha2(col("userId"), 256))
      .withColumn("manufacturer", lit("REDACTED"))
      .withColumn("location", maskStringUdf(col("location")))
      .withColumn("lastUpdated", coarsenTimestampUdf(col("lastUpdated")))
      .withColumn("createdAt", coarsenTimestampUdf(col("createdAt")))
      .withColumn("updatedAt", coarsenTimestampUdf(col("updatedAt")))

      .withColumn(
        "unitModel",
        struct(
          when(col("unitModel.AirQuality").isNotNull,
            struct(
              addNoiseDoubleUdf(col("unitModel.AirQuality.co2Level")).alias("co2Level"),
              addNoiseDoubleUdf(col("unitModel.AirQuality.vocLevel")).alias("vocLevel"),
              col("unitModel.AirQuality.unit").alias("unit")
            )
          )
            .otherwise(col("unitModel.AirQuality"))
            .alias("AirQuality"),

          when(col("unitModel.Humidity").isNotNull,
            col("unitModel.Humidity")
          )
            .otherwise(col("unitModel.Humidity"))
            .alias("Humidity"),

          when(col("unitModel.LightIntensity").isNotNull,
            col("unitModel.LightIntensity")
          )
            .otherwise(col("unitModel.LightIntensity"))
            .alias("LightIntensity"),

          when(col("unitModel.Motion").isNotNull,
            col("unitModel.Motion")
          )
            .otherwise(col("unitModel.Motion"))
            .alias("Motion"),

          when(col("unitModel.Temperature").isNotNull,
            struct(
              addNoiseDoubleUdf(col("unitModel.Temperature.value")).alias("value"),
              col("unitModel.Temperature.unit").alias("unit")
            )
          )
            .otherwise(col("unitModel.Temperature"))
            .alias("Temperature")
        )
      )
  }
}
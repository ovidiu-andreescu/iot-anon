package utils

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object SparkConfig {
    private val config = ConfigFactory.load()

    private val master = config.getString("spark.master")
    private val appName = config.getString("spark.app.name")

    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
}

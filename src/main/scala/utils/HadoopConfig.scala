package utils

import com.typesafe.config.ConfigFactory

object HadoopConfig {
  private val config = ConfigFactory.load()

  object Hadoop {
    val address: String = config.getString("hdfs.address")
  }
  }
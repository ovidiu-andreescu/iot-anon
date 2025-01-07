package utils

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

object KafkaConfig {
  private val config = ConfigFactory.load()

  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap.servers")
    val topic: String = config.getString("kafka.topic")
  }

  def kafkaReadStreamOptions(
                              bootstrapServers: String,
                              topic: String,
                              startingOffsets: String = "latest"
                            ): Map[String, String] = {
    Map(
      "kafka.bootstrap.servers" -> bootstrapServers,
      "subscribe"              -> topic,
      "startingOffsets"        -> startingOffsets
    )
  }

  def getSchemaFromRegistry(topic: String, schemaRegistryUrl: String): String = {
    val subject = s"${topic}-value"
    val client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    val schema = client.getLatestSchemaMetadata(subject).getSchema
    schema
  }
}

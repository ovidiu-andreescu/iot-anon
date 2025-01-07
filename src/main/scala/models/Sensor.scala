package models

final case class Sensor(
                         id: String,
                         serialNumber: String,
                         sensorType: String,
                         model: String,
                         userId: String,
                         location: String,
                         unitModel: UnitModel,
                         status: String = "active",
                         lastUpdated: Long = System.currentTimeMillis(),
                         batteryLevel: Option[Double] = None,
                         connectivity: String = "online",
                         manufacturer: Option[String] = None,
                         firmwareVersion: Option[String] = None,
                         createdAt: Long = System.currentTimeMillis(),
                         updatedAt: Long = System.currentTimeMillis()
                       )

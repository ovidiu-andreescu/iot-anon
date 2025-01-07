package producer

import jobs.StreamingAnonymizationJob.anonymizeSensorDf
import models._
import utils.SparkConfig.sparkSession

class MockDataWriter(outputPath: String) {
  val sensorData: Seq[Sensor] = Seq(
    Sensor(
      id = "sensor-1",
      serialNumber = "SN-1001",
      sensorType = "humidity",
      model = "H-Mod-X",
      userId = "user-1",
      location = "100 Main St, Springfield, ST",
      unitModel = UnitModel(
        Humidity = Some(Humidity(value = 45.2, unit = "%"))
      ),
      status = "active",
      lastUpdated = 1736171000000L,
      batteryLevel = Some(78.4),
      connectivity = "online",
      manufacturer = Some("SensorCorp"),
      firmwareVersion = Some("v1.2"),
      createdAt = 1736169000000L,
      updatedAt = 1736171000000L
    ),
    Sensor(
      id = "sensor-2",
      serialNumber = "SN-2002",
      sensorType = "airQuality",
      model = "AQ-Model-A",
      userId = "user-2",
      location = "200 Air Rd, CleanTown, ST",
      unitModel = UnitModel(
        AirQuality = Some(AirQuality(co2Level = 399.9, vocLevel = 0.15, unit = "ppm"))
      ),
      status = "active",
      lastUpdated = 1736171100000L,
      batteryLevel = None,
      connectivity = "online",
      manufacturer = Some("AcmeAir"),
      firmwareVersion = None,
      createdAt = 1736168000000L,
      updatedAt = 1736171100000L
    ),
    Sensor(
      id = "sensor-3",
      serialNumber = "SN-3003",
      sensorType = "light",
      model = "LX-Model-B",
      userId = "user-3",
      location = "300 Bright Ave, LightCity, ST",
      unitModel = UnitModel(
        LightIntensity = Some(LightIntensity(value = 1240.5, unit = "lux"))
      ),
      status = "active",
      lastUpdated = 1736171200000L,
      batteryLevel = Some(50.0),
      connectivity = "intermittent",
      manufacturer = Some("BrightTech"),
      firmwareVersion = Some("1.1.1"),
      createdAt = 1736169200000L,
      updatedAt = 1736171200000L
    ),
    Sensor(
      id = "sensor-4",
      serialNumber = "SN-4004",
      sensorType = "motion",
      model = "MotionPro-1",
      userId = "user-4",
      location = "400 Security Ln, SafeCity, ST",
      unitModel = UnitModel(
        Motion = Some(Motion(detected = true, timestamp = 1736171300000L))
      ),
      status = "active",
      lastUpdated = 1736171300000L,
      batteryLevel = Some(99.0),
      connectivity = "online",
      manufacturer = Some("MotionMaker"),
      firmwareVersion = Some("2.0"),
      createdAt = 1736169300000L,
      updatedAt = 1736171300000L
    ),
    Sensor(
      id = "sensor-5",
      serialNumber = "SN-5005",
      sensorType = "temperature",
      model = "T-Model-Z",
      userId = "user-5",
      location = "500 Heat St, WarmTown, ST",
      unitModel = UnitModel(
        Temperature = Some(Temperature(value = 22.1, unit = "C"))
      ),
      status = "active",
      lastUpdated = 1736171400000L,
      batteryLevel = Some(12.5),
      connectivity = "offline",
      manufacturer = None,
      firmwareVersion = None,
      createdAt = 1736169400000L,
      updatedAt = 1736171400000L
    )
  )


  def run(): Unit = {
    try {
      import sparkSession.implicits._

      val sensorDf = sensorData.toDF()

      sensorDf.show()

      val anonymizedDf = anonymizeSensorDf(sensorDf)

      anonymizedDf.write.mode("overwrite").parquet(outputPath)

    } catch {
      case e: Exception =>
        println(s"Error running MockData: ${e.getMessage}")
        throw e
    }
  }
}

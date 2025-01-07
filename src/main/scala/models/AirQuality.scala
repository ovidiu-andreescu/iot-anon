package models

final case class AirQuality(co2Level: Double, vocLevel: Double, unit: String = "ppm")

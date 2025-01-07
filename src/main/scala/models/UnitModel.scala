package models

import models.{AirQuality, Humidity, LightIntensity, Motion, Temperature}

final case class UnitModel(
                            AirQuality: Option[AirQuality] = None,
                            Humidity: Option[Humidity] = None,
                            LightIntensity: Option[LightIntensity] = None,
                            Motion: Option[Motion] = None,
                            Temperature: Option[Temperature] = None
                          )

package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters.MapHasAsScala


trait GeoAddSupport extends StrictLogging {

  def getLongitudeField(kcqlConfig: Kcql): String = {
    getStoredAsParameter("longitudeField", kcqlConfig, "longitude")
  }

  def getLatitudeField(kcqlConfig: Kcql): String = {
    getStoredAsParameter("latitudeField", kcqlConfig, "latitude")
  }

  def getStoredAsParameter(parameterName: String, kcqlConfig: Kcql, defaultValue: String): String = {
    val geoAddParams = kcqlConfig.getStoredAsParameters.asScala
    val parameterValue = if (geoAddParams.keys.exists(k => k.equalsIgnoreCase(parameterName)))
      geoAddParams.find { case (k, _) => k.equalsIgnoreCase(parameterName) }.get._2
    else {
      logger.info(s"You have not defined a [$parameterName] field. We'll try to fall back to [$defaultValue] field")
      defaultValue
    }
    parameterValue
  }
}
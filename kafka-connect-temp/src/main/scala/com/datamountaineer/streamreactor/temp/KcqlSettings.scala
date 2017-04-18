package com.datamountaineer.streamreactor.temp

import com.datamountaineer.connector.config.Config
import scala.collection.JavaConversions._

trait KcqlSettings extends BaseSettings {
  val kcqlConstant: String

  def getRoutes: Set[Config] = {
    val raw = getString(kcqlConstant)
    require(!raw.isEmpty, s"$kcqlConstant is empty.")
    raw.split(";").map(r => Config.parse(r)).toSet
  }

  def getFields(routes: Set[Config] = getRoutes): Map[String, Map[String, String]] = {
    routes.map(rm =>
      (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap
  }

  def getIgnoreFields(routes: Set[Config] = getRoutes): Map[String, Set[String]] = {
    routes.map(rm => (rm.getSource, rm.getIgnoredField.toSet)).toMap
  }
}

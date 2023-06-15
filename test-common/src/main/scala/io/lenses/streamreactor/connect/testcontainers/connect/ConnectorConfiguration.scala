package io.lenses.streamreactor.connect.testcontainers.connect

import org.json4s.DefaultFormats
import org.json4s.native.Serialization

case class ConnectorConfiguration(
                                   name:   String,
                                   config: Map[String, ConfigValue[_]],
) {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def toJson(): String = {
    val mergedConfigMap = config + ("tasks.max" -> ConfigValue(1))
    Serialization.write(
      Map[String, Any](
        "name"   -> name,
        "config" -> transformConfigMap(mergedConfigMap),
      ),
    )
  }

  private def transformConfigMap(
                                  originalMap: Map[String, ConfigValue[_]],
  ): Map[String, Any] = originalMap.view.mapValues(_.underlying).toMap

}

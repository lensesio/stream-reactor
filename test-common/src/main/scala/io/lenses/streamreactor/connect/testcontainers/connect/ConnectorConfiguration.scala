package io.lenses.streamreactor.connect.testcontainers.connect

import org.json4s.DefaultFormats
import org.json4s.native.Serialization

case class ConnectorConfiguration(
  name:   String,
  config: Map[String, CnfVal[_]],
) {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def toJson(): String = {
    val mergedConfigMap = config + ("tasks.max" -> IntCnfVal(1))
    Serialization.write(
      Map[String, Any](
        "name"   -> name,
        "config" -> transformConfigMap(mergedConfigMap),
      ),
    )
  }

  private def transformConfigMap(
    originalMap: Map[String, CnfVal[_]],
  ): Map[String, Any] = originalMap.view.mapValues(_.get).toMap

}

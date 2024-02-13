package io.lenses.streamreactor.connect.cloud.common.config.traits

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator

trait CloudPropertiesReader[C <: CloudConfig] {
  def fromProps(props: Map[String, String])(
    implicit connectorTaskId: ConnectorTaskId,
    cloudLocationValidator: CloudLocationValidator
  ): Either[Throwable, C]

}

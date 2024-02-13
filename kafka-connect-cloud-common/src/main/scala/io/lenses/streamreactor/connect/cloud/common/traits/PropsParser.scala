package io.lenses.streamreactor.connect.cloud.common.traits

import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudConnectionConfig

trait PropsParser {

  def parse[CConfig <: CloudConnectionConfig](props: Map[String, String]): Either[Throwable, CConfig]

}

/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.aws.s3.sink.config

import io.lenses.streamreactor.common.config.base.traits._
import io.lenses.streamreactor.connect.aws.s3.config.DeleteModeSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkConfigDefBuilder

import scala.jdk.CollectionConverters.MapHasAsScala

case class S3SinkConfigDefBuilder(props: Map[String, AnyRef])
    extends BaseConfig(S3ConfigSettings.CONNECTOR_PREFIX, S3SinkConfigDef.config, props)
    with CloudSinkConfigDefBuilder
    with ErrorPolicySettings
    with RetryConfigSettings
    with DeleteModeSettings {

  def getParsedValues: Map[String, _] = values().asScala.toMap

}

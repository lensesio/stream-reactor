/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.common.config.base.traits

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.errors
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.errors.ErrorPolicyEnum
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import scala.util.Try

trait ErrorPolicyConfigKey extends WithConnectorPrefix {

  val ERROR_POLICY = s"$connectorPrefix.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC: String =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.s3.max.retries.
      |All errors will be logged automatically, even if the code swallows them.
  """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"
  val ERROR_POLICY_DEFAULT_POLICY: ErrorPolicy = ThrowErrorPolicy()

  def withErrorPolicyConfig(configDef: ConfigDef): ConfigDef =
    configDef.define(
      ERROR_POLICY,
      Type.STRING,
      ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      ERROR_POLICY,
    )

}

trait ErrorPolicySettings extends BaseSettings with ErrorPolicyConfigKey {

  def getErrorPolicy: ErrorPolicy =
    errors.ErrorPolicy(
      ErrorPolicyEnum.withName(getString(ERROR_POLICY).toUpperCase),
    )

  def getErrorPolicyOrDefault: ErrorPolicy = Try(getErrorPolicy).toOption
    .getOrElse(ERROR_POLICY_DEFAULT_POLICY)
}

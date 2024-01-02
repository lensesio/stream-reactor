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

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst._

/**
  * Created by andrew@datamountaineer.com on 31/07/2017.
  * stream-reactor
  */
trait SSLSettings extends BaseSettings {
  val trustStorePath: String = s"$connectorPrefix.$TRUSTSTORE_PATH_SUFFIX"
  val trustStorePass: String = s"$connectorPrefix.$TRUSTSTORE_PASS_SUFFIX"
  val keyStorePath:   String = s"$connectorPrefix.$KEYSTORE_PATH_SUFFIX"
  val keyStorePass:   String = s"$connectorPrefix.$KEYSTORE_PASS_SUFFIX"

}

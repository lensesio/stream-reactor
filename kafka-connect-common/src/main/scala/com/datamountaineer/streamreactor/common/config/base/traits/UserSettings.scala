/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.config.base.traits

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst.{PASSWORD_SUFFIX, USERNAME_SUFFIX}

/**
  * Created by andrew@datamountaineer.com on 29/07/2017. 
  * stream-reactor
  */
trait UserSettings extends BaseSettings {
  val passwordConst = s"$connectorPrefix.$PASSWORD_SUFFIX"
  val usernameConst = s"$connectorPrefix.$USERNAME_SUFFIX"

  def getSecret = getPassword(passwordConst)
  def getUsername = getString(usernameConst)
}

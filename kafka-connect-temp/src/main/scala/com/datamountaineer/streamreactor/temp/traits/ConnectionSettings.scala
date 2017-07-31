/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.temp.traits

/**
  * Created by andrew@datamountaineer.com on 31/07/2017. 
  * stream-reactor
  */

import com.datamountaineer.streamreactor.temp.const.TraitConfigConst.BIND_HOST_SUFFIX
import com.datamountaineer.streamreactor.temp.const.TraitConfigConst.BIND_PORT_SUFFIX
import com.datamountaineer.streamreactor.temp.const.TraitConfigConst.URI_SUFFIX

trait ConnectionSettings extends BaseSettings {
  val bindHostConst = s"$connectorPrefix.$BIND_HOST_SUFFIX"
  val bindPortConst = s"$connectorPrefix.$BIND_PORT_SUFFIX"
  val uriConst = s"$connectorPrefix.$URI_SUFFIX"

  def getBindPort = getInt(bindPortConst)
  def getBindHost = getString(bindHostConst)
  def getUri = getString(uriConst)
}

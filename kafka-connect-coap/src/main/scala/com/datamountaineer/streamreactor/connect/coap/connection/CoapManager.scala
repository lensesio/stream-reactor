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

package com.datamountaineer.streamreactor.connect.coap.connection


import com.datamountaineer.streamreactor.connect.coap.configs.CoapSetting
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.californium.core.{CoapClient, CoapResponse}


/**
  * Created by andrew@datamountaineer.com on 29/12/2016. 
  * stream-reactor
  */
abstract class CoapManager(setting: CoapSetting) extends StrictLogging {

  val client: CoapClient = buildClient

  def buildClient(): CoapClient = {
    val client = DTLSConnectionFn(setting)

    import scala.collection.JavaConverters._
    //discover and check the requested resources
    Option(client.discover())
      .map(_.asScala)
      .getOrElse(Set.empty).map(r => {
      logger.info(s"Discovered resources ${r.getURI}")
      r.getURI
    })

    client.setURI(s"${setting.uri}/${setting.target}")
    client
  }

  def delete(): CoapResponse = client.delete()
}

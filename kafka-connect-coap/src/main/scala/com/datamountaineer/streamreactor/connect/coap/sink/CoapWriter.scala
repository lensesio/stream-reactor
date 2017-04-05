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

package com.datamountaineer.streamreactor.connect.coap.sink

import com.datamountaineer.streamreactor.connect.coap.configs.CoapSetting
import com.datamountaineer.streamreactor.connect.coap.connection.CoapManager
import com.datamountaineer.streamreactor.connect.converters.source.SinkRecordToJson
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.californium.core.CoapResponse
import org.eclipse.californium.core.coap.MediaTypeRegistry

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 29/12/2016. 
  * stream-reactor
  */
class CoapWriter(setting: CoapSetting) extends CoapManager(setting) with ErrorHandler {
  logger.info(s"Initialising CoapWriter for resource ${setting.kcql.getTarget}")

  //initialize error tracker
  initialize(setting.retries.get, setting.errorPolicy.get)

  val fields = Map(setting.kcql.getSource -> setting.kcql.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
  val ignoredFields = Map(setting.kcql.getSource -> setting.kcql.getIgnoredField.toSet)

  def write(records: List[SinkRecord]): Option[Unit] = {
    val responses = Try(records
                      .map(record => SinkRecordToJson(record, fields, ignoredFields))
                      .map(json => (json, client.put(json, MediaTypeRegistry.APPLICATION_JSON)))
                      .filterNot({ case (_, resp) => resp.getCode.codeClass.equals(2) })
                      .foreach({
                        case (json, resp)  =>
                          logger.error(s"Failure sending message $json. Response is ${resp.advanced().getPayload()}, " +
                                s"Code ${resp.getCode.toString}")
                          throw new ConnectException(s"Failure sending message $json. Response is ${resp.advanced().getPayload()}, " +
                            s"Code ${resp.getCode.toString}")
                      }))
    handleTry(responses)
  }

  def stop(): CoapResponse = delete()
}


object CoapWriter {
  def apply(setting: CoapSetting): CoapWriter = new CoapWriter(setting)
}

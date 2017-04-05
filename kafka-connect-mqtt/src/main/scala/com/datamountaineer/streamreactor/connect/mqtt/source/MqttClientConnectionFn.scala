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

package com.datamountaineer.streamreactor.connect.mqtt.source

import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSourceSettings
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttCallback, MqttClient, MqttConnectOptions}

object MqttClientConnectionFn extends StrictLogging {
  def apply(callback: MqttCallback)(implicit settings: MqttSourceSettings): MqttClient = {
    {
      val options = new MqttConnectOptions()
      options.setConnectionTimeout(settings.connectionTimeout)
      options.setKeepAliveInterval(settings.keepAliveInterval)
      options.setCleanSession(settings.cleanSession)
      settings.user.foreach(options.setUserName)
      settings.password.foreach(p => options.setPassword(p.toCharArray))
      options.setAutomaticReconnect(true)
      settings.sslCertFile.foreach { _ =>
        options.setSocketFactory(
          MqttSSLSocketFactory(settings.sslCACertFile.get, settings.sslCertFile.get, settings.sslCertKeyFile.get, "")
        )
      }

      val c = new MqttClient(settings.connection, settings.clientId, new MemoryPersistence())
      c.setCallback(callback)

      logger.info(s"Connecting to ${settings.connection}")
      c.connect(options)
      logger.info(s"Connected to ${settings.connection} as ${settings.clientId}")
      c
    }
  }
}

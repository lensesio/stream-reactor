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

package com.datamountaineer.streamreactor.connect.mqtt.sink

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.sink.Converter
import com.datamountaineer.streamreactor.connect.converters.{FieldConverter, Transform}
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSinkSettings
import com.datamountaineer.streamreactor.connect.mqtt.connection.MqttClientConnectionFn
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 27/08/2017.
  * stream-reactor
  */

object MqttWriter {
  def apply(settings: MqttSinkSettings, convertersMap: Map[String, Converter]): MqttWriter = {
    new MqttWriter(MqttClientConnectionFn.apply(settings), settings, convertersMap)
  }
}

class MqttWriter(client: MqttClient, settings: MqttSinkSettings,
                convertersMap: Map[String, Converter])
  extends StrictLogging with ErrorHandler {

  //initialize error tracker
  implicit val formats = DefaultFormats
  initialize(settings.maxRetries, settings.errorPolicy)
  val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)
  val kcql = settings.kcql
  val msg = new MqttMessage()
  msg.setQos(settings.mqttQualityOfService)
  var mqttTarget : String = ""

  def write(records: Set[SinkRecord]) = {

    val grouped = records.groupBy(r => r.topic())

    val t = Try(grouped.map({
      case (topic, records) =>
        //get the kcql statements for this topic
        val kcqls: Set[Kcql] = mappings.get(topic).get
        kcqls.map(k => {
          //for all the records in the group transform
          records.map(r => {
            val transformed = Transform(
              k.getFields.asScala.map(FieldConverter.apply),
              k.getIgnoredFields.asScala.map(FieldConverter.apply),
              r.valueSchema(),
              r.value(),
              k.hasRetainStructure
            )

            //get kafka message key if asked for
            if (!Option(k.getDynamicTarget).getOrElse("").isEmpty) {
              var mqtttopic = (parse(transformed) \ k.getDynamicTarget).extractOrElse[String](null)
              if (mqtttopic.nonEmpty) {
                mqttTarget = mqtttopic
              }
            } else if (k.getTarget == "_key") {
              mqttTarget = r.key().toString()
            } else {
              mqttTarget = k.getTarget
            }

            val converter = convertersMap.getOrElse(k.getSource, null)
            val value = if (converter == null) {
              transformed.getBytes()
            } else {
              val converted_record = converter.convert(mqttTarget, r)
              converted_record.value().asInstanceOf[Array[Byte]]
            }

            (mqttTarget, value)
          }).map(
            {
              case (t, value) => {
                msg.setPayload(value)
                msg.setRetained(settings.mqttRetainedMessage);

                client.publish(t, msg)

              }
            }
          )
        })
    }))

    //handle any errors according to the policy.
    handleTry(t)
  }

  def flush = {}

  def close = {
    client.disconnect()
    client.close()
  }
}

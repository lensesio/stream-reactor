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

package com.datamountaineer.streamreactor.connect.bloomberg

import com.datamountaineer.streamreactor.connect.bloomberg.config.BloombergSourceConfig._
import com.datamountaineer.streamreactor.connect.bloomberg.config.{BloombergSourceConfigConstants, BloombergSourceConfig}

import scala.util.{Success, Try}

/**
  * Contains all the Bloomberg connection details and the subscriptions to be created
  *
  * @param serverHost         : The Bloomberg server name
  * @param serverPort         : The port number to connect to
  * @param serviceUri         : Which Bloomberg service to connect to. i.e.
  * @param subscriptions      : A list of subscription information. Each subscription contains the ticker and the fields
  * @param kafkaTopic         : The kafka topic on which the SourceRecords will be sent to
  * @param authenticationMode : There are two modes available APPLICATION_ONLY or USER_AND_APPLICATION. See the Bloomberg
  *                           documentation for details
  * @param bufferSize         : Specifies how large is the buffer accumulating the updates from Bloomberg
  */
case class BloombergSettings(serverHost: String,
                             serverPort: Int,
                             serviceUri: String,
                             subscriptions: Seq[SubscriptionInfo],
                             kafkaTopic: String,
                             authenticationMode: Option[String] = None,
                             bufferSize: Int = 2048,
                             payloadType: PayloadTye = JsonPayload) {

  def asMap(): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put(BloombergSourceConfigConstants.SERVER_HOST, serverHost)
    map.put(BloombergSourceConfigConstants.SERVER_PORT, serverPort.toString)
    map.put(BloombergSourceConfigConstants.SERVICE_URI, serviceUri)
    map.put(BloombergSourceConfigConstants.SUBSCRIPTIONS, subscriptions.map(_.toString).mkString(";"))
    map.put(BloombergSourceConfigConstants.KAFKA_TOPIC, kafkaTopic.toString)
    authenticationMode.foreach(v => map.put(BloombergSourceConfigConstants.AUTHENTICATION_MODE, v))
    map.put(BloombergSourceConfigConstants.BUFFER_SIZE, bufferSize.toString)
    map.put(BloombergSourceConfigConstants.PAYLOAD_TYPE, payloadType.toString)
    map
  }
}

object BloombergSettings {
  /**
    * Creates an instance of BloombergSettings from a BloombergSourceConfig
    *
    * @param connectorConfig : The map of all provided configurations
    * @return An instance of BloombergSettings
    */
  def apply(connectorConfig: BloombergSourceConfig): BloombergSettings = {
    val serverHost = connectorConfig.getString(BloombergSourceConfigConstants.SERVER_HOST)
    require(serverHost.trim.nonEmpty, "Server host should be defined")

    val serverPort = connectorConfig.getInt(BloombergSourceConfigConstants.SERVER_PORT)
    require(serverPort > 0 && serverPort < Short.MaxValue, "Invalid port number")

    val bloombergService = connectorConfig.getString(BloombergSourceConfigConstants.SERVICE_URI)
    require(BloombergSourceConfigConstants.BloombergServicesUris.contains(bloombergService),
      s"Invalid service uri. Supported ones are ${BloombergSourceConfigConstants.BloombergServicesUris.mkString(",")}"
    )

    val subscriptions = SubscriptionInfoExtractFn(connectorConfig.getString(BloombergSourceConfigConstants.SUBSCRIPTIONS))
    require(subscriptions.nonEmpty, "Need to provide at least one subscription information")

    val authenticationMode = Try(connectorConfig.getString(BloombergSourceConfigConstants.AUTHENTICATION_MODE)).toOption

    val kafkaTopic = connectorConfig.getString(BloombergSourceConfigConstants.KAFKA_TOPIC)

    val bufferSize: Int = Try(connectorConfig.getInt(BloombergSourceConfigConstants.BUFFER_SIZE)) match {
      case Success(v) => v
      case _ => BloombergConstants.Default_Buffer_Size
    }

    val payloadType = Try(connectorConfig.getString(BloombergSourceConfigConstants.PAYLOAD_TYPE)).toOption match {
      case None => JsonPayload
      case Some(v) => v match {
        case "json" => JsonPayload
        case "avro" => AvroPayload
      }
    }

    new BloombergSettings(serverHost,
      serverPort,
      bloombergService,
      subscriptions,
      kafkaTopic,
      authenticationMode,
      bufferSize,
      payloadType)
  }
}

/**
  * Defines the way the source serializes the data sent over Kafka. There are two modes available: json and avro
  */
sealed trait PayloadTye

case object JsonPayload extends PayloadTye {
  override def toString: String = "json"
}

case object AvroPayload extends PayloadTye {
  override def toString: String = "avro"
}

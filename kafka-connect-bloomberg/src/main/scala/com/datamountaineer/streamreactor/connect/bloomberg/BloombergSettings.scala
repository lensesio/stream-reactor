package com.datamountaineer.streamreactor.connect.bloomberg

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
    import ConnectorConfig._

    val map = new java.util.HashMap[String, String]()
    map.put(SERVER_HOST, serverHost)
    map.put(SERVER_PORT, serverPort.toString)
    map.put(SERVICE_URI, serviceUri)
    map.put(SUBSCRIPTIONS, subscriptions.map(_.toString).mkString(";"))
    map.put(KAFKA_TOPIC, kafkaTopic.toString)
    authenticationMode.foreach(v => map.put(AUTHENTICATION_MODE, v))
    map.put(BUFFER_SIZE, bufferSize.toString)
    map.put(PAYLOAD_TYPE, payloadType.toString)
    map
  }
}

object BloombergSettings {
  /**
    * Creates an instance of BloombergSettings from a ConnectorConfig
    *
    * @param connectorConfig : The map of all provided configurations
    * @return An instance of BloombergSettings
    */
  def apply(connectorConfig: ConnectorConfig): BloombergSettings = {
    val serverHost = connectorConfig.getString(ConnectorConfig.SERVER_HOST)
    require(serverHost.trim.nonEmpty, "Server host should be defined")

    val serverPort = connectorConfig.getInt(ConnectorConfig.SERVER_PORT)
    require(serverPort > 0 && serverPort < Short.MaxValue, "Invalid port number")

    val bloombergService = connectorConfig.getString(ConnectorConfig.SERVICE_URI)
    require(ConnectorConfig.BloombergServicesUris.contains(bloombergService),
      s"Invalid service uri. Supported ones are ${ConnectorConfig.BloombergServicesUris.mkString(",")}"
    )

    val subscriptions = SubscriptionInfoExtractFn(connectorConfig.getString(ConnectorConfig.SUBSCRIPTIONS))
    require(subscriptions.nonEmpty, "Need to provide at least one subscription information")

    val authenticationMode = Try(connectorConfig.getString(ConnectorConfig.AUTHENTICATION_MODE)).toOption

    val kafkaTopic = connectorConfig.getString(ConnectorConfig.KAFKA_TOPIC)

    val bufferSize: Int = Try(connectorConfig.getInt(ConnectorConfig.BUFFER_SIZE)) match {
      case Success(v) => v
      case _ => BloombergConstants.Default_Buffer_Size
    }

    val payloadType = Try(connectorConfig.getString(ConnectorConfig.PAYLOAD_TYPE)).toOption match {
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
  override def toString = "json"
}

case object AvroPayload extends PayloadTye {
  override def toString = "avro"
}
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

import java.util

import com.bloomberglp.blpapi._
import com.datamountaineer.streamreactor.connect.bloomberg.config.BloombergSourceConfig
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

/**
  * <h1>BloombergSourceTask</h1>
  *
  * Kafka Connect Cassandra source task. Called by framework to get the records to be sent over kafka to the sink
  **/
class BloombergSourceTask extends SourceTask with StrictLogging {
  var settings: Option[BloombergSettings] = None

  var subscriptions: Option[SubscriptionList] = None
  var session: Option[Session] = None

  var subscriptionManager: Option[BloombergSubscriptionManager] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  
  /**
    * Un-subscribes the tickers and stops the Bloomberg session
    */
  override def stop(): Unit = {
    logger.info(s"Shutting down Bloomberg source for subscriptions ${subscriptions.get.asScala.mkString(",")}")
    try {
      session.get.unsubscribe(subscriptions.get)
    }
    catch {
      case _: Throwable =>
        logger.error(s"Unexpected exception un-subscribing for correlation=${CorrelationIdsExtractorFn(subscriptions.get)}")
    }
    try {
      session.get.stop()
    }
    catch {
      case _: InterruptedException =>
        logger.error(s"There was an error stopping the bloomberg session for correlation=${CorrelationIdsExtractorFn(subscriptions.get)}")
    }
    session = None
    subscriptions.get.clear()
    subscriptions = None
    subscriptionManager = None
  }

  /**
    * Creates and starts the Bloomberg session and subscribes for the tickers data update
    *
    * @param map A map of configuration properties for this task
    */
  override def start(map: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/bloomberg-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    try {
      settings = Some(BloombergSettings(new BloombergSourceConfig(map)))
      subscriptions = Some(SubscriptionsBuilderFn(settings.get))

      val correlationToTicketMap = subscriptions.get.asScala.map { s => s.correlationID().value() -> s.subscriptionString() }.toMap
      subscriptionManager = Some(new BloombergSubscriptionManager(correlationToTicketMap))
      session = Some(BloombergSessionCreateFn(settings.get, subscriptionManager.get))

      val authorization = settings.get.serviceAuthorization
      if(authorization) {
        val identity = BloombergSessionAuthorizationFn(session.get)
        session.get.subscribe(subscriptions.get, identity)
      } else {
        session.get.subscribe(subscriptions.get)
      }
    }
    catch {
      case t: Throwable => throw new ConnectException("Could not start the task because of invalid configuration.", t)
    }
  }

  override def version: String = manifest.version()

  /**
    * Called by the framework. It returns all the accumulated records since the previous call.
    *
    * @return A list of records as a result of Bloomberg updates since the previous call.
    */
  override def poll(): util.List[SourceRecord] = {
    subscriptionManager.get.getData.map { d =>
      val list = new util.ArrayList[SourceRecord](d.size())
      d.asScala.foreach { d =>
        list.add(d.toSourceRecord(settings.get))
      }
      list
    }.orNull
  }
}

object SubscriptionsBuilderFn extends StrictLogging {
  /**
    * Creates a list of subscriptions to be made to Bloomberg
    *
    * @param settings : The connector settings containing all the subscription information
    * @return The instance of Bloomberg subscriptions
    */
  def apply(settings: BloombergSettings): SubscriptionList = {
    val subscriptions = new SubscriptionList()

    settings.subscriptions.zipWithIndex.foreach { case (config, i) =>
      val unrecognizedFields = config.fields.map(_.toUpperCase).filterNot(BloombergConstants.SubscriptionFields.contains)
      if (unrecognizedFields.nonEmpty) {
        throw new IllegalArgumentException(s"Following fields are not recognized: ${unrecognizedFields.mkString(",")}")
      }

      /*
      val fields = config.fields.map(_.trim.toUpperCase).mkString(",")
      logger.debug(s"Creating a Bloomberg subscription for ${config.ticket} with $fields and correlation:$i")
      val subscription = new Subscription(config.ticket, fields, new CorrelationID(i))
      */
      // Topic is needed for ticker, otherwise, //blp/mktdata will be used as default
      val topic = settings.serviceUri
      val subscriptionString = if(config.subscription.startsWith("/")) topic + config.subscription else topic + '/' + config.subscription
      logger.info(s"Creating a Bloomberg subscription for $subscriptionString and correlation:$i")
      val subscription = new Subscription(subscriptionString, new CorrelationID(i))
      subscriptions.add(subscription)
    }
    logger.info(s"Created subscriptions for ${subscriptions.asScala.mkString(",")}")
    subscriptions
  }
}

object BloombergSessionCreateFn extends StrictLogging {
  /**
    * Creates and starts a Bloomberg session and connects to the appropriate Bloomberg service (market data, reference data)
    *
    * @param settings : Contains all the connection details for the Bloomberg session
    * @param handler  : Instance of EventHandler providing the callbacks for Bloomberg events
    * @return The Bloomberg session
    */
  def apply(settings: BloombergSettings, handler: EventHandler): Session = {
    val options = new SessionOptions
    options.setKeepAliveEnabled(true)
    options.setServerHost(settings.serverHost)
    options.setServerPort(settings.serverPort)
    settings.authenticationMode.foreach(options.setAuthenticationOptions)

    logger.info("Starting session.")
    val session = new Session(options, handler)

    if (!session.start()) {
      sys.error(s"Could not start the session for ${settings.serverHost}:${settings.serverPort}")
    }

    // For subscription, don't need open service.  Service is indicated as prefix in Subscription.
    /*
    if (!session.openService(settings.serviceUri)) {
      sys.error(s"Could not open service ${settings.serviceUri}")
    }
    */
    session
  }
}

object BloombergSessionAuthorizationFn extends StrictLogging {
  /**
    * Perform authentication and authorization for a bloomberg session.
    *
    * @param session : a bloomberg started session
    * @return Identity for bloomberg session
    */
  def apply(session: Session) : Identity = {
    val tokenEventQueue = new EventQueue();
    try {
      session.generateToken(new CorrelationID("generateTokenID"), tokenEventQueue);
    }
    catch {
      case t: Throwable => logger.error("Generate token failed")
    }
    var token: String = null;
    val timeoutMilliSeconds = 10000;
    val event = tokenEventQueue.nextEvent(timeoutMilliSeconds);
    if (event.eventType().intValue == Event.EventType.Constants.TOKEN_STATUS) {
    val iter = event.messageIterator();
    while (iter.hasNext()) {
        var msg = iter.next();
        if (msg.messageType().toString == "TokenGenerationSuccess") {
          token = msg.getElementAsString("token");
        }
      }
    }
    if (token == null) {
	    sys.error("Failed to get token");
    }

    val apiIdentity = session.createIdentity();
    if (session.openService("//blp/apiauth")) {
      val authService = session.getService("//blp/apiauth");
      val authRequest = authService.createAuthorizationRequest();
      authRequest.set("token", token);

      logger.info("Sending out authRequest with token[{}]", token);
      val eventQueue = new EventQueue();
      session.sendAuthorizationRequest(authRequest, apiIdentity, eventQueue, new CorrelationID(apiIdentity));
      breakable {  
        while (true) {
          val authEvent = eventQueue.nextEvent();
          val eventType = authEvent.eventType().intValue
          if (eventType == Event.EventType.Constants.RESPONSE || eventType == Event.EventType.Constants.PARTIAL_RESPONSE
            || eventType == Event.EventType.Constants.REQUEST_STATUS) {
            val msgIter = authEvent.messageIterator();
            while (msgIter.hasNext()) {
              val msg = msgIter.next();
              logger.info(s"---- Authorization [$msg] ----");
              if (msg.messageType().toString == "AuthorizationSuccess") {
                logger.info("---- Authorization successful ----");
                break;
              } else {
                sys.error(s"---- Authorization failed msg [$msg] ----");
                break;
              }
            } // while (msgIter.hasNext())
          }
        } // while (true)
      } // breakable
    } // if (session.openService("//blp/apiauth"))
    apiIdentity
  }
}

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

package com.datamountaineer.streamreactor.connect.jms

import java.util.Properties
import javax.jms._
import javax.naming.InitialContext

import com.datamountaineer.streamreactor.connect.jms.config.DestinationSelector.DestinationSelector
import com.datamountaineer.streamreactor.connect.jms.config._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 10/03/2017. 
  * stream-reactor
  */
case class JMSSessionProvider(queueConsumers: Map[String, MessageConsumer],
                              topicsConsumers: Map[String, MessageConsumer],
                              queueProducers : Map[String, MessageProducer],
                              topicProducers : Map[String, MessageProducer],
                              session: Session,
                              connection: Connection,
                              context: InitialContext) extends StrictLogging {

  def start(): Unit = {
    logger.info(s"Starting connection to ${connection.toString}")
    connection.start()
  }

  def close(): Try[Unit] = {
    topicsConsumers.foreach({ case(source, consumer) =>
      logger.info(s"Stopping topic consumer for $source")
      consumer.close()
    })
    queueConsumers.foreach({ case(source, consumer) =>
      logger.info(s"Stopping queue consumer for $source")
      consumer.close()
    })

    topicProducers.foreach({ case(source, producer) =>
      logger.info(s"Stopping topic producer for $source")
      producer.close()
    })

    queueProducers.foreach({ case(source, producer) =>
      logger.info(s"Stopping queue producer for $source")
      producer.close()
    })

    Try(session.close())
    Try(context.close())
  }
}

object JMSSessionProvider extends StrictLogging {
  def apply(settings: JMSSettings, sink: Boolean = false): JMSSessionProvider ={
    val context =  new InitialContext(getProps(settings))
    val connectionFactory = context.lookup(settings.connectionFactoryClass).asInstanceOf[ConnectionFactory]
    val connection = settings.user match {
      case None => connectionFactory.createConnection()
      case Some(user) => connectionFactory.createConnection(user, settings.password.get.value())
    }

    val session = Try(connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) match {
      case Success(s) =>
        logger.info("Created session")
        s
      case Failure(f) =>
        logger.error("Failed to create session", f.getMessage)
        throw new ConnectException(f)
    }

    val topicsConsumers = configureDestination(TopicDestination, context, session, settings, sink)
                            .flatMap({ case (source, topic) => createConsumers(source, session, topic, settings.subscriptionName)}).toMap

    val queueConsumers = configureDestination(QueueDestination, context, session, settings, sink)
                            .flatMap({ case (source, queue) => createConsumers(source, session, queue, settings.subscriptionName) }).toMap


    val topicProducers = configureDestination(TopicDestination, context, session, settings, sink)
                            .flatMap({ case (source, topic) => createProducers(source, session, topic) }).toMap

    val queueProducers = configureDestination(QueueDestination, context, session, settings, sink)
                            .flatMap({ case (source, queue) => createProducers(source, session, queue) }).toMap

    new JMSSessionProvider(queueConsumers, topicsConsumers, queueProducers, topicProducers, session, connection, context)
  }

  def configureDestination(destinationType: DestinationType, context: InitialContext, session: Session, settings: JMSSettings, sink: Boolean = false): List[(String, Destination)] = {
    settings.settings
      .filter(f => f.destinationType.equals(destinationType))
      .map(t => (t.source, getDestination(if (sink) t.target else t.source, context, settings.destinationSelector, destinationType, session)))
  }

  def createProducers(source: String, session: Session, destination: Destination) = Map(source -> session.createProducer(destination))

  def createConsumers(source: String, session: Session, destination: Destination, subscriptionName: Option[String]) = Map(source -> {
    subscriptionName match {
      case None =>  session.createConsumer(destination)
      case Some(name) => 
        destination match {
          case destination: Topic => session.createSharedDurableConsumer(destination, name)
          case _ => session.createConsumer(destination)
        }
    }
  })

  private def getDestination(name: String, context: InitialContext, selector: DestinationSelector,
                             destination: DestinationType, session: Session): Destination = {
    selector match {
      case DestinationSelector.JNDI => context.lookup(name).asInstanceOf[Destination]
      case DestinationSelector.CDI =>
        destination match {
          case QueueDestination => session.createQueue(name)
          case TopicDestination => session.createTopic(name)
        }
    }
  }

  /**
    * Construct the properties for the initial context
    * adding in anything in the extra properties for solace or ibm
    *
    * */
  private def getProps(settings: JMSSettings) = {
    val props = new Properties()
    props.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, settings.initialContextClass)
    props.setProperty(javax.naming.Context.PROVIDER_URL, settings.connectionURL)
    props.setProperty(javax.naming.Context.SECURITY_PRINCIPAL, settings.user.getOrElse(""))
    props.setProperty(javax.naming.Context.SECURITY_AUTHENTICATION, settings.password.getOrElse(new Password("")).value())
    settings.extraProps.map(e => e.map { case (k,v) => props.setProperty(k, v) })
    props
  }
}

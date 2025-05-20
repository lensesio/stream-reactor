/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.jms

import io.lenses.streamreactor.connect.jms.config.DestinationSelector.DestinationSelector

import io.lenses.streamreactor.connect.jms.config.DestinationSelector
import io.lenses.streamreactor.connect.jms.config.DestinationType
import io.lenses.streamreactor.connect.jms.config.JMSSettings
import io.lenses.streamreactor.connect.jms.config.QueueDestination
import io.lenses.streamreactor.connect.jms.config.TopicDestination
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import java.util.Properties
import jakarta.jms._
import javax.naming.InitialContext
import scala.util.control.NonFatal
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 10/03/2017.
  * stream-reactor
  */
case class JMSSessionProvider(
  queueConsumers:  Map[String, MessageConsumer],
  topicsConsumers: Map[String, MessageConsumer],
  queueProducers:  Map[String, MessageProducer],
  topicProducers:  Map[String, MessageProducer],
  session:         Session,
  connection:      Connection,
  context:         InitialContext,
) extends StrictLogging {

  def start(): Unit = {
    logger.info(s"Starting connection to ${connection.toString}")
    connection.start()
  }

  def close(): Try[Unit] =
    for {
      _ <- Try(connection.close())
      _ <- Try(context.close())
    } yield ()
}

object JMSSessionProvider extends StrictLogging {
  def apply(settings: JMSSettings, sink: Boolean = false): JMSSessionProvider =
    withConnection(settings) { (context, connection) =>
      val session = Try(connection.createSession(sink, Session.CLIENT_ACKNOWLEDGE)) match {
        case Success(s) =>
          logger.info("Created session")
          s
        case Failure(f) =>
          logger.error("Failed to create session", f.getMessage)
          throw new ConnectException(f)
      }

      if (sink) {
        val topicProducers = configureDestination(TopicDestination, context, session, settings, sink)
          .flatMap({ case (source, _, _, topic) => createProducers(source, session, topic) }).toMap

        val queueProducers = configureDestination(QueueDestination, context, session, settings, sink)
          .flatMap({ case (source, _, _, queue) => createProducers(source, session, queue) }).toMap
        new JMSSessionProvider(Map[String, MessageConsumer](),
                               Map[String, MessageConsumer](),
                               queueProducers,
                               topicProducers,
                               session,
                               connection,
                               context,
        )
      } else {
        val topicsConsumers = configureDestination(TopicDestination, context, session, settings, sink)
          .flatMap(
            {
              case (source, messageSelector, subscriptionName, topic) =>
                createConsumers(source, session, topic, subscriptionName, messageSelector)
            },
          ).toMap
        val queueConsumers = configureDestination(QueueDestination, context, session, settings, sink)
          .flatMap(
            {
              case (source, messageSelector, subscriptionName, queue) =>
                createConsumers(source, session, queue, subscriptionName, messageSelector)
            },
          ).toMap
        new JMSSessionProvider(queueConsumers,
                               topicsConsumers,
                               Map[String, MessageProducer](),
                               Map[String, MessageProducer](),
                               session,
                               connection,
                               context,
        )
      }
    }

  def configureDestination(
    destinationType: DestinationType,
    context:         InitialContext,
    session:         Session,
    settings:        JMSSettings,
    sink:            Boolean = false,
  ): List[(String, Option[String], Option[String], Destination)] =
    settings.settings
      .filter(f => f.destinationType.equals(destinationType))
      .map(t =>
        (t.source,
         t.messageSelector,
         t.subscriptionName,
         getDestination(if (sink) t.target else t.source,
                        context,
                        settings.destinationSelector,
                        destinationType,
                        session,
         ),
        ),
      )

  def createProducers(source: String, session: Session, destination: Destination) =
    Map(source -> session.createProducer(destination))

  def createConsumers(
    source:           String,
    session:          Session,
    destination:      Destination,
    subscriptionName: Option[String],
    messageSelector:  Option[String],
  ) = Map(
    source -> {
      val selector = messageSelector.orNull
      subscriptionName match {
        case None => session.createConsumer(destination, selector)
        case Some(name) =>
          destination match {
            case destination: Topic => session.createSharedDurableConsumer(destination, name, selector)
            case _ => session.createConsumer(destination, selector)
          }
      }
    },
  )

  private def withConnection[T](settings: JMSSettings)(f: (InitialContext, Connection) => T): T = {
    val context: InitialContext = new InitialContext(getProps(settings))
    val connectionFactory = context.lookup(settings.connectionFactoryClass).asInstanceOf[ConnectionFactory]
    val connection = settings.user match {
      case None       => connectionFactory.createConnection()
      case Some(user) => connectionFactory.createConnection(user, settings.password.get.value())
    }
    try {
      f(context, connection)
    } catch {
      case NonFatal(e) =>
        connection.close()
        throw e
    }
  }

  private def getDestination(
    name:        String,
    context:     InitialContext,
    selector:    DestinationSelector,
    destination: DestinationType,
    session:     Session,
  ): Destination =
    selector match {
      case DestinationSelector.JNDI => context.lookup(name).asInstanceOf[Destination]
      case DestinationSelector.CDI =>
        destination match {
          case QueueDestination => session.createQueue(name)
          case TopicDestination => session.createTopic(name)
        }
    }

  /**
    * Construct the properties for the initial context
    * adding in anything in the extra properties for solace or ibm
    */
  private def getProps(settings: JMSSettings) = {
    val props = new Properties()
    props.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, settings.initialContextClass)
    props.setProperty(javax.naming.Context.PROVIDER_URL, settings.connectionURL)
    props.setProperty(javax.naming.Context.SECURITY_PRINCIPAL, settings.user.getOrElse(""))
    props.setProperty(javax.naming.Context.SECURITY_CREDENTIALS, settings.password.getOrElse(new Password("")).value())
    settings.extraProps.map(e => e.map { case (k, v) => props.setProperty(k, v) })
    props
  }
}

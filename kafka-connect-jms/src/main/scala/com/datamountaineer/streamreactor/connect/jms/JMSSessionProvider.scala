package com.datamountaineer.streamreactor.connect.jms

import java.util.Properties
import javax.naming.InitialContext
import javax.jms._

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
case class JMSProvider(queueConsumers: Map[String, MessageConsumer],
                  topicsConsumers: Map[String, MessageConsumer],
                  session: Session,
                  connection: Connection,
                  context: InitialContext
                 ) extends StrictLogging {


  def close() = {
    topicsConsumers.foreach({ case(source, consumer) =>
      logger.info(s"Stopping queue consumer for ${source}")
      consumer.close()
    })
    queueConsumers.foreach({ case(source, consumer) =>
      logger.info(s"Stopping consumer for ${source}")
      consumer.close()
    })
    Try(session.close())
    Try(session.close())
    Try(context.close())
  }
}

object JMSProvider extends StrictLogging {
  def apply(settings: JMSSettings): JMSProvider ={
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

    val topicsConsumers = settings
      .settings
      .filter(f => f.destinationType.equals(TopicDestination))
      .map(t => (t.source, getDestination(t.source, context, settings.destinationSelector, TopicDestination, session)))
      .map({ case (source, topic) => (source, session.createConsumer(topic)) })
      .toMap

    val queueConsumers = settings
      .settings
      .filter(f => f.destinationType.equals(QueueDestination))
      .map(t => (t.source,getDestination(t.source, context, settings.destinationSelector, QueueDestination, session)))
      .map({ case (source, queue) => (source, session.createConsumer(queue)) })
      .toMap

    new JMSProvider(queueConsumers, topicsConsumers, session, connection, context)
  }

  private def getDestination(name: String, context: InitialContext, selector: DestinationSelector, destination: DestinationType, session: Session): Destination = {
    selector match {
      case DestinationSelector.JNDI => context.lookup(name).asInstanceOf[Destination]
      case DestinationSelector.CDI => {
        destination match {
          case QueueDestination => session.createQueue(name)
          case TopicDestination => session.createTopic(name)
        }
      }
    }
  }

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

package com.datamountaineer.streamreactor.connect.jms

import java.util.Properties
import javax.jms.{Connection, Destination, Session}
import javax.naming.InitialContext

import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSSettings, QueueDestination, TopicDestination}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 10/03/2017. 
  * stream-reactor
  */
class JMSProvider(settings: JMSSettings, session: Session, connection: Connection, context: InitialContext) extends StrictLogging {

  val topicsConsumers = settings
                .settings
                .filter(f => f.destinationType.equals(TopicDestination))
                .map(t => (t.source, getTopics(t.source)))
                .map({ case (source, topic) => (source, session.createConsumer(topic)) })

  val queueConsumers = settings
                .settings
                .filter(f => f.destinationType.equals(QueueDestination))
                .map(t => (t.source, getQueues(t.source)))
                .map({ case (source, queue) => (source, session.createConsumer(queue)) })

  private def getTopics(name: String): Destination = {
    settings.destinationSelector match {
      case DestinationSelector.JNDI => context.lookup(name).asInstanceOf[Destination]
      case DestinationSelector.CDI => session.createTopic(name)
    }
  }

  private def getQueues(name: String) : Destination = {
    settings.destinationSelector match {
      case DestinationSelector.JNDI => context.lookup(name).asInstanceOf[Destination]
      case DestinationSelector.CDI => session.createQueue(name)
    }
  }

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
    Try(connection.close())
    Try(context.close())
  }
}

object JMSProvider extends StrictLogging {
  def apply(settings: JMSSettings): JMSProvider ={
    val context = settings.destinationSelector match {
      case DestinationSelector.CDI => new InitialContext(getProps(settings))
      case _ => new InitialContext(new Properties())
    }

    val connectionFactory = settings.connectionFactoryClass.getConstructor(classOf[String]).newInstance()
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

    new JMSProvider(settings, session, connection, context)
  }

  private def getProps(settings: JMSSettings) = {
    val props = new Properties()
    props.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, settings.connectionFactoryClass.getCanonicalName)
    props.setProperty(javax.naming.Context.PROVIDER_URL, settings.connectionURL)
    props.setProperty(javax.naming.Context.SECURITY_PRINCIPAL, settings.user.getOrElse(""))
    props.setProperty(javax.naming.Context.SECURITY_AUTHENTICATION, settings.password.getOrElse(new Password("")).value())
    props
  }
}

package com.datamountaineer.streamreactor.connect.bloomberg

import java.util
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import com.bloomberglp.blpapi._
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.JavaConverters._

/**
  * Provides the callback functions for Bloomberg events. It listens to the SUBSCRIPTION_DATA events and extracts and stores
  * the updates.
  */
class BloombergSubscriptionManager(correlationToSubscriptionMap: Map[Long, String], bufferSize: Int = 2048) extends EventHandler with StrictLogging {
  val queue: BlockingQueue[BloombergData] = new ArrayBlockingQueue[BloombergData](bufferSize)

  def getData: Option[util.LinkedList[BloombergData]] = {
    logger.debug("Draining the buffer queue....")
    if (queue.isEmpty) {
      logger.debug("Nothing in the buffer was found.")
      None
    }
    else {
      val list = new util.LinkedList[BloombergData]()
      queue.drainTo(list)
      logger.debug(s"${list.size()} items have been taken from the buffer.")
      Some(list)
    }
  }

  override def processEvent(event: Event, session: Session): Unit = {
    event.eventType().intValue() match {
      case Event.EventType.Constants.SUBSCRIPTION_DATA => onDataEvent(event, session)

      case Event.EventType.Constants.SESSION_STATUS |
           Event.EventType.Constants.SERVICE_STATUS |
           Event.EventType.Constants.SUBSCRIPTION_STATUS => onStatusEvent(event, session)

      case _ => onOtherEvent(event, session);
    }
  }

  /**
    * Handles status updates. For now all it does is logging them
    *
    * @param event
    * @param session
    */
  private def onStatusEvent(event: Event, session: Session) = {
    event.iterator().asScala.foreach { message =>
      logger.debug(s"On topic ${message.topicName()} received status event:${message.messageType().toString} with correlation id=${message.correlationID}")
    }
  }

  /**
    * Handles data update event. It extracts all the information contained by the Bloomberg message and will append it
    * to the buffer. If the buffer is full the call is blocking
    *
    * @param event   : The Bloomberg event containing the updates
    * @param session : The instance to the Bloomberg session
    */
  private def onDataEvent(event: Event, session: Session) = {
    event.iterator().asScala.foreach { message =>
      val element = message.asElement()
      val fieldsNo = element.numElements()
      val correlation = message.correlationID().value()
      logger.debug(s"Received subscription data event for correlation id=$correlation with $fieldsNo fields")

      correlationToSubscriptionMap.get(correlation) match {
        case None => logger.warn(s"Received an unmatched correlation id: $correlation. All available correlation ids are $correlationToSubscriptionMap")
        case Some(s) =>
          val fields = (0 until fieldsNo)
            .map(element.getElement)
            .filter(f => !f.isNull)
            .foldLeft(new util.HashMap[String, Any]()) { case (map, f) =>
              map.put(f.name().toString, BloombergFieldValueFn(f))
              map
            }
          queue.add(BloombergData(s, fields))
      }
    }
  }

  /**
    * Handles non status and non data events. All it does is logging them
    *
    * @param event
    * @param session
    */
  private def onOtherEvent(event: Event, session: Session) = {
    logger.trace(s"Ignoring event:${event.eventType().toString}...")
  }
}
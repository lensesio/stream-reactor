package com.datamountaineer.streamreactor.connect.coap.source

import org.apache.kafka.connect.source.SourceRecord
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.actor.ActorRef
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.util


/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object ActorHelper extends StrictLogging {

  def askForRecords(actorRef: ActorRef) : util.ArrayList[SourceRecord] = {
    implicit val timeout = akka.util.Timeout(10.seconds)
    Await.result((actorRef ? (DataRequest))
      .map(_.asInstanceOf[util.ArrayList[SourceRecord]])
      .recoverWith { case t =>
        logger.error("Could not retrieve the source records", t)
        Future.successful(new util.ArrayList[SourceRecord]())
      }, Duration.Inf)
  }
}
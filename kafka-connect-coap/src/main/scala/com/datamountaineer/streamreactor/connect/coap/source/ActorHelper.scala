/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

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
    Await.result((actorRef ? DataRequest)
      .map(_.asInstanceOf[util.ArrayList[SourceRecord]])
      .recoverWith { case t =>
        logger.error("Could not retrieve the source records", t)
        Future.successful(new util.ArrayList[SourceRecord]())
      }, Duration.Inf)
  }
}
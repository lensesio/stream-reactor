/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.schemas

import com.typesafe.scalalogging.StrictLogging
import io.confluent.kafka.schemaregistry.client.rest.RestService

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 13/06/16.
  * kafka-connect-common
  */
object SchemaRegistry extends StrictLogging {

  /**
    * Get a schema for a given subject
    *
    * @param url The url of the schema registry
    * @param subject The subject to het the schema for
    * @return The schema for the subject
    */
  def getSchema(url: String, subject: String): String = {
    val registry = new RestService(url)

    Try(registry.getLatestVersion(subject).getSchema) match {
      case Success(s) =>
        logger.info(s"Found schema for $subject")
        s
      case Failure(throwable) =>
        logger.warn(
          "Unable to connect to the Schema registry. An attempt will be made to create the table on receipt of the first records.",
          throwable,
        )
        ""
    }
  }

  /**
    * Get a list of subjects from the registry
    *
    * @param url The url to the schema registry
    * @return A list of subjects/topics
    */
  def getSubjects(url: String): List[String] = {
    val registry = new RestService(url)
    val schemas: List[String] = Try(registry.getAllSubjects.asScala.toList) match {
      case Success(s) => s
      case Failure(_) => {
        logger.warn(
          "Unable to connect to the Schema registry. An attempt will be made to create the table" +
            " on receipt of the first records.",
        )
        List.empty[String]
      }
    }

    schemas.foreach(s => logger.info(s"Found schemas for $s"))
    schemas
  }
}

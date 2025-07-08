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
package io.lenses.streamreactor.common.config
import io.lenses.streamreactor.common.utils.EitherOps._
import io.lenses.kcql.Kcql
import org.apache.kafka.common.config.ConfigException
import com.typesafe.scalalogging.StrictLogging

/**
 * Created by andrew@datamountaineer.com on 13/05/16.
 * kafka-connect-common
 */

object Helpers extends StrictLogging {

  def checkInputTopics(kcqlConstant: String, props: Map[String, String]): Boolean =
    checkInputTopicsEither(kcqlConstant, props).unpackOrThrow

  def checkInputTopicsEither(kcqlConstant: String, props: Map[String, String]): Either[ConfigException, Boolean] =
    for {
      topics <- props.get("topics")
        .map(_.split(",").map(_.trim).toSet)
        .toRight(new ConfigException("Missing `topics` configuration"))
      raw <- props.get(kcqlConstant)
        .filter(_.nonEmpty)
        .toRight(new ConfigException(s"Missing $kcqlConstant"))
      kcql    = raw.split(";").map(Kcql.parse).toSet
      sources = kcql.map(_.getSource)

      _ <- Either.cond(
        topics.subsetOf(sources),
        (),
        new ConfigException(
          s"Mandatory `topics` configuration contains topics not set in $kcqlConstant: ${topics.diff(sources)}, kcql contains $sources",
        ),
      )

      _ <- Either.cond(
        sources.subsetOf(topics),
        (),
        new ConfigException(
          s"$kcqlConstant configuration contains topics not set in mandatory `topic` configuration: ${sources.diff(topics)}, kcql contains $sources",
        ),
      )
    } yield true
}

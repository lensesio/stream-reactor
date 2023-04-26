/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.common.config.base.traits

import cats.implicits.catsSyntaxEitherId
import ch.qos.logback.classic.Level
import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst._
import org.apache.kafka.connect.errors.ConnectException
import org.slf4j.LoggerFactory

import scala.util.Try

trait LogOverrideSettings extends BaseSettings {

  private val CONNECTOR_LOG_LEVEL_OVERRIDE = s"$connectorPrefix.$LOG_LEVEL_OVERRIDE"
  private val PACKAGES                     = Set("io.lenses", "com.datamountaineer")

  def configureLoggingOverrides(): Either[Throwable, Unit] = {
    val lvl = getString(CONNECTOR_LOG_LEVEL_OVERRIDE).toUpperCase
    if (lvl != "DEFAULT") {
      val (bad, _) = PACKAGES.map(pkg => setLevel(lvl, pkg)).partitionMap(identity)
      bad.headOption.map(new ConnectException("Unable to configure logging level (first err)", _)).toLeft(())
    } else ().asRight
  }
  private def setLevel(level: String, pkg: String): Either[Throwable, Unit] =
    Try {
      val _             = LoggerFactory.getLogger(pkg)
      val classicLogger = org.slf4j.LoggerFactory.getLogger(pkg).asInstanceOf[ch.qos.logback.classic.Logger]
      classicLogger.setLevel(Level.toLevel(level))
    }.toEither

}

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

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Using

class LogOverrideSettingsTest extends AnyFlatSpecLike with Matchers with LazyLogging {

  "logOverrideSettings" should "be able to disable logging" in {
    val testLoggerConfig = new TestLoggerConfig(Map("cp.log.level.override" -> "OFF"))
    testLoggerConfig.configureLoggingOverrides()

    Using.resource(new LogWatcher) {
      lw =>
        logSomething()
        lw.getLogs() should be(empty)
    }
  }

  "logOverrideSettings" should "be able to enable debug logging" in {
    val testLoggerConfig = new TestLoggerConfig(Map("cp.log.level.override" -> "DEBUG"))
    testLoggerConfig.configureLoggingOverrides()

    Using.resource(new LogWatcher) {
      lw =>
        logSomething()
        lw.getLogs() should have size 1
        lw.getLogs().head.getMessage should be("Something")
    }
  }

  class LogWatcher extends AutoCloseable {

    private val listAppender = createListAppender()
    getDMLogger().addAppender(listAppender)

    def getLogs(): Seq[ILoggingEvent] = listAppender.list.asScala.toSeq

    override def close(): Unit = listAppender.stop

    private def createListAppender(): ListAppender[ILoggingEvent] = {
      val listAppender = new ListAppender[ILoggingEvent]
      listAppender.start()
      listAppender
    }
    private def getDMLogger() = LoggerFactory.getLogger("com.datamountaineer").asInstanceOf[Logger]
  }

  private def logSomething() =
    logger.debug("Something")

}

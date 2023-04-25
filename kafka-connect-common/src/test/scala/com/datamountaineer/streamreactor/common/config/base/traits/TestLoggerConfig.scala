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

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.types.Password
import org.scalatest.Assertions.fail

import java.lang
import java.util

class TestLoggerConfig(props: Map[String, String]) extends LogOverrideSettings with LazyLogging {
  override def connectorPrefix: String = "cp"

  override def getString(key: String): String = props.get(key).head

  override def getInt(key: String): Integer = fail("Unimplemented")

  override def getBoolean(key: String): lang.Boolean = fail("Unimplemented")

  override def getPassword(key: String): Password = fail("Unimplemented")

  override def getList(key: String): util.List[String] = fail("Unimplemented")

}

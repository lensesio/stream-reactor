/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.opensearch

import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Unit tests for [[OpenSearchSinkConnector]] covering:
 *  - topics-vs-kcql consistency
 *  - taskConfigs distributes the config to all task slots
 *  - taskClass returns [[OpenSearchSinkTask]]
 *  - stop() is a no-op
 */
class OpenSearchSinkConnectorTest extends AnyFunSuite with Matchers {

  private val baseProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
  )

  private def startConnector(extra: Map[String, String]): OpenSearchSinkConnector = {
    val connector = new OpenSearchSinkConnector
    connector.start((baseProps ++ extra).asJava)
    connector
  }

  test("taskClass returns OpenSearchSinkTask") {
    val connector = startConnector(Map(KCQL -> "INSERT INTO idx SELECT * FROM topic", "topics" -> "topic"))
    connector.taskClass() shouldBe classOf[OpenSearchSinkTask]
    connector.stop()
  }

  test("taskConfigs returns one config map per requested task slot") {
    val connector = startConnector(Map(KCQL -> "INSERT INTO idx SELECT * FROM topic", "topics" -> "topic"))
    val configs   = connector.taskConfigs(3)
    configs should have size 3
    configs.asScala.foreach { cfg =>
      cfg.get(KCQL) shouldBe "INSERT INTO idx SELECT * FROM topic"
    }
    connector.stop()
  }

  test("topics-vs-kcql mismatch — source topic not in KCQL raises at connector start") {
    // Helpers.checkInputTopics validates that each topic in 'topics' has a corresponding KCQL source.
    val ex = intercept[Exception](
      startConnector(Map(
        KCQL     -> "INSERT INTO idx SELECT * FROM topic-a",
        "topics" -> "topic-a,topic-b", // topic-b has no KCQL source
      )),
    )
    ex should not be null
    ex.getMessage.toLowerCase should (include("topic-b") or include("topics") or include("kcql"))
  }

  test("matching topics and KCQL sources — connector starts successfully") {
    noException shouldBe thrownBy {
      startConnector(Map(
        KCQL     -> "INSERT INTO a SELECT * FROM topic-a;INSERT INTO b SELECT * FROM topic-b",
        "topics" -> "topic-a,topic-b",
      ))
    }
  }

  test("stop() does not throw") {
    val connector = startConnector(Map(KCQL -> "INSERT INTO idx SELECT * FROM topic", "topics" -> "topic"))
    noException shouldBe thrownBy(connector.stop())
  }

  test("bidirectional mismatch — KCQL source topic not in topics raises at connector start") {
    // Reverse of the previous case: all 'topics' have a KCQL source, but the KCQL also mentions
    // a source that is not in 'topics'. This should also be rejected.
    val ex = intercept[Exception](
      startConnector(
        Map(
          KCQL     -> "INSERT INTO a SELECT * FROM topic-a;INSERT INTO orphan SELECT * FROM topic-orphan",
          "topics" -> "topic-a", // topic-orphan is in KCQL but not in 'topics'
        ),
      ),
    )
    ex should not be null
    ex.getMessage.toLowerCase should (include("topic-orphan") or include("topics") or include("kcql"))
  }

  test("config() returns the OpenSearch ConfigDef") {
    val connector = new OpenSearchSinkConnector
    connector.config() should not be null
  }
}

/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink
import io.circe.parser.decode
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.http.sink.client.BasicAuthentication
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod.Put
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class HttpSinkConfigTest extends AnyFunSuiteLike with Matchers with EitherValues {

  test("should write config to json") {
    HttpSinkConfig(
      Put,
      "http://myaddress.example.com",
      "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
      Some(BasicAuthentication("user", "pass")),
      Seq("something" -> "somethingelse").some,
      none,
      none,
      none,
      none,
    ).toJson should be(
      """{"method":"Put","endpoint":"http://myaddress.example.com","content":"<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>","authentication":{"username":"user","password":"pass","type":"BasicAuthentication"},"headers":[["something","somethingelse"]],"sslConfig":null,"batch":null,"errorThreshold":null,"uploadSyncPeriod":null}""",
    )
  }

  test("read minimal config") {
    val minConfig =
      """{"method":"Put","endpoint":"http://myaddress.example.com","content":"<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>"}"""
    val minConfigSink = decode[HttpSinkConfig](minConfig)
    minConfigSink.value should be(
      HttpSinkConfig(
        Put,
        "http://myaddress.example.com",
        "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        none,
        none,
        none,
        none,
        none,
        none,
      ),
    )
  }
}

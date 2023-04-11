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
package io.lenses.streamreactor.connect.aws.s3.config

import cats.implicits.catsSyntaxOptionId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionPoolConfigTest extends AnyFlatSpec with Matchers {

  "ConnectionPoolConfig" should "ignore -1" in {
    ConnectionPoolConfig(Option(-1)) should be(Option.empty)
  }

  "ConnectionPoolConfig" should "ignore empty" in {
    ConnectionPoolConfig(Option.empty) should be(Option.empty)
  }

  "ConnectionPoolConfig" should "be good with a positive int" in {
    ConnectionPoolConfig(Some(5)) should be(ConnectionPoolConfig(5).some)
  }
}

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
package io.lenses.streamreactor.connect.http.sink.client

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.Decoder
import io.circe.Encoder

sealed trait Authentication

object Authentication {
  implicit val customConfig: Configuration = Configuration.default.withDiscriminator("type")

  implicit val decoder: Decoder[Authentication] = deriveConfiguredDecoder[Authentication]
  implicit val encoder: Encoder[Authentication] = deriveConfiguredEncoder[Authentication]
}

case class BasicAuthentication(username: String, password: String) extends Authentication

object BasicAuthentication {
  implicit val customConfig: Configuration = Configuration.default.withDiscriminator("type")

  implicit val decoder: Decoder[BasicAuthentication] = deriveConfiguredDecoder
  implicit val encoder: Encoder[BasicAuthentication] = deriveConfiguredEncoder
}

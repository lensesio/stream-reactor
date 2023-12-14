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

import cats.effect.IO
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.tpl.templates.ProcessedTemplate
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString
class HttpRequestSender(config: HttpSinkConfig) {

  def sendHttpRequest(
    client:            Client[IO],
    processedTemplate: ProcessedTemplate,
  ): IO[Unit] = {

    val uri = Uri.unsafeFromString(processedTemplate.endpoint)

    val clientHeaders: Headers = Headers(processedTemplate.headers.map {
      case (name, value) =>
        Header.ToRaw.rawToRaw(new Header.Raw(CIString(name), value))
    }: _*)

    val request = Request[IO](
      method  = Method.fromString(config.method.entryName).getOrElse(Method.GET),
      uri     = uri,
      headers = clientHeaders,
    ).withEntity(config.content)

    // Add authentication if present
    val authenticatedRequest = config.authentication.fold(request) {
      case BasicAuthentication(username, password) =>
        request.putHeaders(Authorization(BasicCredentials(username, password)))
    }

    for {
      response <- client.expect[String](authenticatedRequest)
      _        <- IO(println(s"Response: $response"))
    } yield ()

  }

}

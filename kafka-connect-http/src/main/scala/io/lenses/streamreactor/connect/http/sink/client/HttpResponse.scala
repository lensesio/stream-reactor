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
package io.lenses.streamreactor.connect.http.sink.client

import io.lenses.streamreactor.common.exception.StreamReactorException

/**
  * Represents a successful HTTP response.
  *
  * @param statusCode the HTTP status code of the response
  * @param responseContent the content of the response
  */
case class HttpResponseSuccess(
  statusCode:      Int,
  responseContent: Option[String],
)

/**
  * Represents a failed HTTP response.
  *
  * @param message the error message
  * @param cause the optional cause of the failure
  * @param statusCode the optional HTTP status code of the response
  * @param responseContent the optional content of the response
  */
case class HttpResponseFailure(
  message:         String,
  cause:           Option[Throwable],
  statusCode:      Option[Int],
  responseContent: Option[String],
) extends StreamReactorException(message, cause.orNull)

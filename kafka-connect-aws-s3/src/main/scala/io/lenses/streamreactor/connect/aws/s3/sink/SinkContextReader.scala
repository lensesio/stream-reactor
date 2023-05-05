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
package io.lenses.streamreactor.connect.aws.s3.sink

import java.util
import scala.util.Try

object SinkContextReader {

  def getProps(
    contextPropsFn: () => util.Map[String, String],
  )(fallbackProps:  util.Map[String, String],
  ): Either[String, util.Map[String, String]] =
    choosePropsMap(
      Try(contextPropsFn()).toOption,
      Option(fallbackProps),
    ).toRight("No props found")

  private def choosePropsMap(
    propsMaps: Option[util.Map[String, String]]*,
  ): Option[util.Map[String, String]] =
    propsMaps.filter(_ != null).find(_.nonEmpty).flatten

}

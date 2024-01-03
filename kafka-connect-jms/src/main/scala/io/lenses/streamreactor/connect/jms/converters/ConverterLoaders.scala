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
package io.lenses.streamreactor.connect.jms.converters

import cats.implicits._
import io.lenses.kcql.FormatType
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.jms.sink.converters.JMSMessageConverterFn
import io.lenses.streamreactor.connect.jms.sink.converters.JMSSinkMessageConverter
import org.apache.kafka.common.config.ConfigException

import scala.util.Try

object ConverterLoaders {

  def connectorPropertyConverterLoader[C <: JMSMessageConverter](
    classNameKey: String,
  )(
    implicit
    converterLoader: ConverterClassLoader[C],
    props:           Map[String, String],
  ): Option[Either[ConfigException, C]] =
    props.get(classNameKey).map(converterLoader.load(_, props))

  private def loadFormatTypeConverter(
    formatType: FormatType,
    props:      Map[String, String],
  ): Option[Either[ConfigException, JMSSinkMessageConverter]] =
    Some(
      Try {
        val converter = JMSMessageConverterFn(formatType)
        converter.initialize(props)
        converter
      }.toEither.leftMap(e => new ConfigException(e.getMessage)),
    )

  def kcqlSinkFormatTypeConverterLoader[C <: JMSMessageConverter](
  )(
    implicit
    props: Map[String, String],
    kcql:  Kcql,
  ): Option[Either[ConfigException, JMSSinkMessageConverter]] =
    Option(kcql.getFormatType).flatMap {
      ft: FormatType =>
        for {
          formatTypeConverter <- loadFormatTypeConverter(ft, props)
        } yield formatTypeConverter
    }

  def kcqlConverterClassNameConverterLoader[C <: JMSMessageConverter](
  )(
    implicit
    converterLoader: ConverterClassLoader[C],
    props:           Map[String, String],
    kcql:            Kcql,
  ): Option[Either[ConfigException, C]] =
    Option(kcql.getWithConverter)
      .filter(_.nonEmpty)
      .map {
        converterLoader.load(_, props)
      }

  def defaultKcqlConverterLoader[C <: JMSMessageConverter](
    defaultClass: Class[_],
  )(
    implicit
    converterLoader: ConverterClassLoader[C],
    props:           Map[String, String],
  ): Option[Either[ConfigException, C]] =
    Some(converterLoader.load(defaultClass.getName, props))
}

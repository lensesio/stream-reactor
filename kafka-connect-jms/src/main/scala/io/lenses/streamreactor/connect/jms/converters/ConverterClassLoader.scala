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
package io.lenses.streamreactor.connect.jms.converters

import cats.implicits._
import io.lenses.streamreactor.connect.converters.source.Converter
import io.lenses.streamreactor.connect.jms.config.JMSConfigConstants
import io.lenses.streamreactor.connect.jms.sink.converters.JMSSinkMessageConverter
import io.lenses.streamreactor.connect.jms.source.converters.CommonJMSMessageConverter
import io.lenses.streamreactor.connect.jms.source.converters.JMSSourceMessageConverter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigException

import scala.util.Try

/**
  * The ConverterClassLoader is responsible for loading and initialising converters.
  * @tparam C supertype of the resultant converter
  */
trait ConverterClassLoader[C <: JMSMessageConverter] extends LazyLogging {

  def load(className: String, props: Map[String, String]): Either[ConfigException, C] = {
    logger.info(s"Creating converter instance for $className")
    for {
      loaded <- Try(Class.forName(className).getDeclaredConstructor().newInstance()).toEither
        .leftMap(_ =>
          new ConfigException(s"Invalid ${JMSConfigConstants.KCQL} is invalid. $className should have an empty ctor!"),
        )
      converter <- verifyClassWithinConstraints(loaded)
      _          = converter.initialize(props)
    } yield converter
  }

  protected def verifyClassWithinConstraints(loaded: Any): Either[ConfigException, C]
}

class SourceConverterClassLoader extends ConverterClassLoader[JMSSourceMessageConverter] {

  protected override def verifyClassWithinConstraints(loaded: Any): Either[ConfigException, JMSSourceMessageConverter] =
    loaded match {
      case cls: JMSSourceMessageConverter =>
        cls.asRight
      case com: Converter =>
        new CommonJMSMessageConverter(com).asRight
      case _ =>
        new ConfigException("Invalid class, expected C").asLeft
    }
}

class SinkConverterClassLoader extends ConverterClassLoader[JMSSinkMessageConverter] {
  override protected def verifyClassWithinConstraints(loaded: Any): Either[ConfigException, JMSSinkMessageConverter] =
    loaded match {
      case cls: JMSSinkMessageConverter => cls.asRight
      case _ => new ConfigException("Invalid class, expected the appropriate type").asLeft
    }
}

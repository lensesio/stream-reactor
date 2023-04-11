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
package com.datamountaineer.streamreactor.connect.jms.config

import cats.implicits.catsSyntaxEitherId
import com.datamountaineer.streamreactor.connect.jms.sink.converters.JMSSinkMessageConverter
import com.datamountaineer.streamreactor.connect.jms.source.converters.JMSSourceMessageConverter

/**
  * ConverterConfigWrapper holds the configured converter.  There are only 2 implementations, for source or Sink.  This ensures that only one converter can be configured at a given time for a KCQL row.
  */
trait ConverterConfigWrapper {
  def forSource: Either[String, JMSSourceMessageConverter] = "Configured sink, requested source".asLeft

  def forSink: Either[String, JMSSinkMessageConverter] = "Configured source, requested sink".asLeft
}

case class SourceConverterConfigWrapper(converter: JMSSourceMessageConverter) extends ConverterConfigWrapper {
  override def forSource: Either[String, JMSSourceMessageConverter] = converter.asRight
}

case class SinkConverterConfigWrapper(converter: JMSSinkMessageConverter) extends ConverterConfigWrapper {
  override def forSink: Either[String, JMSSinkMessageConverter] = converter.asRight
}

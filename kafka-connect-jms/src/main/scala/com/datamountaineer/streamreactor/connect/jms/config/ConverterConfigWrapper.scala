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

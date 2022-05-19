package com.datamountaineer.streamreactor.connect.jms.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.jms.converters.ConverterLoaders.{connectorPropertyConverterLoader, defaultKcqlConverterLoader, kcqlConverterClassNameConverterLoader, kcqlSinkFormatTypeConverterLoader}
import com.datamountaineer.streamreactor.connect.jms.converters.{ConverterClassLoader, JMSMessageConverter, SinkConverterClassLoader, SourceConverterClassLoader}
import com.datamountaineer.streamreactor.connect.jms.sink.converters.{JMSSinkMessageConverter, JsonMessageConverter}
import com.datamountaineer.streamreactor.connect.jms.source.converters.{JMSSourceMessageConverter, JMSStructMessageConverter}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigException

trait ConverterConfigurator[C <: JMSMessageConverter] extends LazyLogging {

  protected val converterLoader: ConverterClassLoader[C]

  protected def wrap(c: C): ConverterConfigWrapper

}

object JMSConnectorConverters {

  /**
    * Enable configuration of converters
    * @param sink whether this is a sink
    * @param kcql the corresponding row from KCQL
    * @param props properties from the connector
    * @return the configured converter based on the precedence rules.
    *         Precedence Rules:
    *         <ol>
    *           <li>The value defined in the connector properties JMSConfigConstants.DEFAULT_SINK_CONVERTER_CONFIG or JMSConfigConstants.DEFAULT_SOURCE_CONVERTER_CONFIG</li>
    *           <li>The converter associated with Kcql.getFormatType (sinks only)</li>
    *           <li>The converter defined in Kcql.getWithConverter</li>
    *           <li>A "Default" KCQL converter (for sink this is defined as JSONMessageConverter, for source JMSStructMessageConverter)</li>
    *         </ol>
    */
  def apply(
    sink: Boolean,
  )(
    implicit
    kcql:  Kcql,
    props: Map[String, String],
  ): Option[Either[ConfigException, ConverterConfigWrapper]] =
    if (sink) SinkConverters() else SourceConverters()
}

object SinkConverters extends ConverterConfigurator[JMSSinkMessageConverter] {

  override implicit protected val converterLoader: ConverterClassLoader[JMSSinkMessageConverter] =
    new SinkConverterClassLoader

  override protected def wrap(c: JMSSinkMessageConverter): ConverterConfigWrapper = SinkConverterConfigWrapper(c)

  def apply(
  )(
    implicit
    kcql:  Kcql,
    props: Map[String, String],
  ): Option[Either[ConfigException, ConverterConfigWrapper]] =
    sinkConverterLoaderChain.map(_.map(wrap))

  def sinkConverterLoaderChain(
    implicit
    p: Map[String, String],
    k: Kcql,
  ): Option[Either[ConfigException, JMSSinkMessageConverter]] =
    connectorPropertyConverterLoader(JMSConfigConstants.DEFAULT_SINK_CONVERTER_CONFIG) orElse
      kcqlSinkFormatTypeConverterLoader() orElse
      kcqlConverterClassNameConverterLoader() orElse
      defaultKcqlConverterLoader(classOf[JsonMessageConverter])

}

object SourceConverters extends ConverterConfigurator[JMSSourceMessageConverter] {

  override implicit protected val converterLoader: ConverterClassLoader[JMSSourceMessageConverter] =
    new SourceConverterClassLoader

  override protected def wrap(c: JMSSourceMessageConverter): ConverterConfigWrapper = SourceConverterConfigWrapper(c)

  def apply(
  )(
    implicit
    kcql:  Kcql,
    props: Map[String, String],
  ): Option[Either[ConfigException, ConverterConfigWrapper]] =
    sourceConverterLoaderChain.map(_.map(wrap))

  def sourceConverterLoaderChain(
    implicit
    p: Map[String, String],
    k: Kcql,
  ): Option[Either[ConfigException, JMSSourceMessageConverter]] =
    connectorPropertyConverterLoader(JMSConfigConstants.DEFAULT_SOURCE_CONVERTER_CONFIG) orElse
      kcqlConverterClassNameConverterLoader() orElse
      defaultKcqlConverterLoader(classOf[JMSStructMessageConverter])

}

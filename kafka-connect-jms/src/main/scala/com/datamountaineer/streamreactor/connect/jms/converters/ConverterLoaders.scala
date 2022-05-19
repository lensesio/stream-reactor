package com.datamountaineer.streamreactor.connect.jms.converters

import cats.implicits._
import com.datamountaineer.kcql.FormatType
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.jms.sink.converters.JMSMessageConverterFn
import com.datamountaineer.streamreactor.connect.jms.sink.converters.JMSSinkMessageConverter
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

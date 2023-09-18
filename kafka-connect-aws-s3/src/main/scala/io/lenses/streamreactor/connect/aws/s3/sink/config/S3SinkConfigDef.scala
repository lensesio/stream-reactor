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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import cats.implicits.catsSyntaxEitherId
import com.datamountaineer.streamreactor.common.config.base.traits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.aws.s3.config.processors.ConfigDefProcessor
import io.lenses.streamreactor.connect.aws.s3.config.processors.DeprecationConfigDefProcessor
import io.lenses.streamreactor.connect.aws.s3.config.processors.LowerCaseKeyConfigDefProcessor
import io.lenses.streamreactor.connect.aws.s3.config.processors.YamlProfileProcessor
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import java.util
import scala.jdk.CollectionConverters._
import S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.sink.config.padding.PaddingStrategySettings

object S3SinkConfigDef {

  val config: ConfigDef = CommonConfigDef.config
    .define(
      DISABLE_FLUSH_COUNT,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      "Disable flush on reaching count",
    )
    .define(
      LOCAL_TMP_DIRECTORY,
      Type.STRING,
      "",
      Importance.LOW,
      s"Local tmp directory for preparing the files",
    )
    .define(
      SEEK_MAX_INDEX_FILES,
      Type.INT,
      SEEK_MAX_INDEX_FILES_DEFAULT,
      Importance.LOW,
      SEEK_MAX_INDEX_FILES_DOC,
      "Sink Seek",
      2,
      ConfigDef.Width.LONG,
      SEEK_MAX_INDEX_FILES,
    )
    .define(
      PADDING_STRATEGY,
      Type.STRING,
      PADDING_STRATEGY_DEFAULT,
      Importance.LOW,
      PADDING_STRATEGY_DOC,
    )
    .define(
      PADDING_LENGTH,
      Type.INT,
      PADDING_LENGTH_DEFAULT,
      Importance.LOW,
      PADDING_LENGTH_DOC,
    )
}

class S3SinkConfigDef() extends ConfigDef with LazyLogging {

  private val processorChain: List[ConfigDefProcessor] =
    List(new LowerCaseKeyConfigDefProcessor, new DeprecationConfigDefProcessor, new YamlProfileProcessor)

  override def parse(jProps: util.Map[_, _]): util.Map[String, AnyRef] = {
    val scalaProps: Map[Any, Any] = jProps.asScala.toMap
    processProperties(scalaProps) match {
      case Left(exception) => throw exception
      case Right(value)    => super.parse(value.asJava)
    }
  }

  private def processProperties(scalaProps: Map[Any, Any]): Either[Throwable, Map[Any, Any]] = {
    val stringProps    = scalaProps.collect { case (k: String, v: AnyRef) => (k.toLowerCase, v) }
    val nonStringProps = scalaProps -- stringProps.keySet
    processStringKeyedProperties(stringProps) match {
      case Left(exception)         => exception.asLeft[Map[Any, Any]]
      case Right(stringKeyedProps) => (nonStringProps ++ stringKeyedProps).asRight
    }
  }

  def processStringKeyedProperties(stringProps: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    var remappedProps: Map[String, Any] = stringProps
    for (proc <- processorChain) {
      proc.process(remappedProps) match {
        case Left(exception)   => return exception.asLeft[Map[String, AnyRef]]
        case Right(properties) => remappedProps = properties
      }
    }
    remappedProps.asRight
  }

}

case class S3SinkConfigDefBuilder(props: util.Map[String, String])
    extends BaseConfig(S3ConfigSettings.CONNECTOR_PREFIX, S3SinkConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings
    with S3FlushSettings
    with CompressionCodecSettings
    with PaddingStrategySettings
    with DeleteModeSettings {

  def getParsedValues: Map[String, _] = values().asScala.toMap

}

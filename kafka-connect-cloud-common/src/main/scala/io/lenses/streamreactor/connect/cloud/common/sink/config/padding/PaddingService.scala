/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.config.padding

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PaddingCharacter
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PaddingLength
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PaddingSelection
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingType.LeftPad
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties.stringToInt
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties.stringToString

object PaddingService {

  val DefaultPaddingStrategy: PaddingType = LeftPad
  val DefaultPadLength:       Int         = 12
  val DefaultPadChar:         Char        = '0'
  val OffsetField:            String      = "offset"
  val DefaultPadFields:       Set[String] = Set(OffsetField)

  private trait PaddingConfigDetector[C] {

    def configApplied(config: C): Boolean

    def processConfig(config: C): Either[Throwable, PaddingService]

  }

  private object ConfigDefPaddingConfigDetector extends PaddingConfigDetector[PaddingStrategySettings] {
    override def configApplied(config: PaddingStrategySettings): Boolean = config.getPaddingStrategy.nonEmpty

    override def processConfig(config: PaddingStrategySettings): Either[Throwable, PaddingService] =
      config.getPaddingStrategy.map(ps => new PaddingService(Map(OffsetField -> ps))).get.asRight
  }

  private object KcqlPropsPaddingConfigDetector
      extends PaddingConfigDetector[KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]] {
    override def configApplied(config: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Boolean =
      config.containsKeyStartingWith("padding.")

    override def processConfig(
      config: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
    ): Either[Throwable, PaddingService] = {
      val maybeFields = config.getOptionalMap[String, Int](PaddingLength, stringToString, stringToInt)
      val maybeType   = config.getEnumValue[PaddingType, PaddingType.type](PaddingType, PaddingSelection)
      for {
        _ <- requireOffsetWhenFieldsConfigured(maybeFields)
        _ <- requireOffsetPaddingEnabled(maybeFields, maybeType)
      } yield fromDefaults(config.getOptionalChar(PaddingCharacter), maybeFields, maybeType)
    }

    private def requireOffsetWhenFieldsConfigured(maybeFields: Option[Map[String, Int]]): Either[Throwable, Unit] =
      maybeFields.fold(().asRight[Throwable]) {
        fields =>
          Either.cond(
            fields.contains(OffsetField),
            (),
            new IllegalArgumentException(
              s"KCQL property ${PaddingLength.entryName} must include an $OffsetField entry (for example ${PaddingLength.entryName}.$OffsetField=12); " +
                s"got: ${fields.keys.mkString(", ")}.",
            ),
          )
      }

    private def requireOffsetPaddingEnabled(
      maybeFields: Option[Map[String, Int]],
      maybeType:   Option[PaddingType],
    ): Either[Throwable, Unit] =
      maybeFields.fold(().asRight[Throwable]) { _ =>
        Either.cond(
          !maybeType.contains(PaddingType.NoOp),
          (),
          new IllegalArgumentException(
            s"KCQL property ${PaddingSelection.entryName}=${PaddingType.NoOp.entryName} disables $OffsetField padding " +
              s"while a ${PaddingLength.entryName} map is configured; remove the map or choose a padding type that pads $OffsetField.",
          ),
        )
      }

    private def fromDefaults(
      maybeChar:   Option[Char],
      maybeFields: Option[Map[String, Int]],
      maybeType:   Option[PaddingType],
    ): PaddingService = {
      val fields: Map[String, Int] = maybeFields.getOrElse(DefaultPadFields.map(f => f -> DefaultPadLength)).toMap
      val pChar:  Char             = maybeChar.getOrElse(DefaultPadChar)
      val pType:  PaddingType      = maybeType.getOrElse(DefaultPaddingStrategy)
      val paddingStrategies = fields.map(f => f._1 -> pType.toPaddingStrategy(f._2, pChar))
      new PaddingService(paddingStrategies)
    }

  }
  def fromConfig(
    confDef: PaddingStrategySettings,
    props:   KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): Either[Throwable, PaddingService] = {
    val cdConf = ConfigDefPaddingConfigDetector.configApplied(confDef)
    val kpConf = KcqlPropsPaddingConfigDetector.configApplied(props)
    (cdConf, kpConf) match {
      case (true, true) =>
        new IllegalStateException(
          "Unable to process both padding Kafka Connect config properties and KCQL properties.  Please use one or the other.  We recommend KCQL properties for additional configuration ability.",
        ).asLeft
      case (true, false) => ConfigDefPaddingConfigDetector.processConfig(confDef)
      case _             => KcqlPropsPaddingConfigDetector.processConfig(props)
    }

  }

}

class PaddingService(
  fields: Map[String, PaddingStrategy],
) {

  def padderFor(field: String): PaddingStrategy =
    fields.get(field) match {
      case Some(fieldPaddingStrategy) => fieldPaddingStrategy
      case None                       => NoOpPaddingStrategy
    }
}

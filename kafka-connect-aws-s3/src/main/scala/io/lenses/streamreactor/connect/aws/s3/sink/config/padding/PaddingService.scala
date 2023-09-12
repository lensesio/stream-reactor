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
package io.lenses.streamreactor.connect.aws.s3.sink.config.padding

import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEnum.PaddingCharacter
import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEnum.PaddingFields
import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEnum.PaddingLength
import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEnum.PaddingSelection
import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEntry
import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.aws.s3.sink.PaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfigDefBuilder
import io.lenses.streamreactor.connect.aws.s3.sink.config.padding.PaddingType.LeftPad
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

import scala.reflect.ClassTag

trait PaddingService {

  def getPadderForField(field: String): String => String

}

object DefaultPaddingService {

  val DefaultPadFields:       Set[String] = Set("offset")
  val DefaultPaddingStrategy: PaddingType = LeftPad
  val DefaultPadLength:       Int         = 12
  val DefaultPadChar:         Char        = '0'

  implicit def stringToString: String => String = identity[String]

  implicit val stringClassTag: ClassTag[String] = ClassTag(classOf[String])

  def fromConfig(
    confDef: S3SinkConfigDefBuilder,
    props:   KcqlProperties[S3PropsKeyEntry, S3PropsKeyEnum.type],
  ): PaddingService = {
    val maybeKcqlPaddingSvc = confDef.getPaddingStrategy.map(ps => new DefaultPaddingService(DefaultPadFields, ps))
    maybeKcqlPaddingSvc.getOrElse(fromProps(props))
  }

  private def fromDefaults(
    maybeChar:   Option[Char],
    maybeLength: Option[Int],
    maybeFields: Option[Set[String]],
    maybeType:   Option[PaddingType],
  ): PaddingService = {
    val fields: Set[String] = maybeFields.getOrElse(DefaultPadFields)
    val length: Int         = maybeLength.getOrElse(DefaultPadLength)
    val pChar:  Char        = maybeChar.getOrElse(DefaultPadChar)
    val pType:  PaddingType = maybeType.getOrElse(DefaultPaddingStrategy)
    val pS = pType.toPaddingStrategy(length, pChar)
    new DefaultPaddingService(fields, pS)
  }
  private def fromProps(props: KcqlProperties[S3PropsKeyEntry, S3PropsKeyEnum.type]): PaddingService =
    fromDefaults(
      props.getOptionalChar(PaddingCharacter),
      props.getOptionalInt(PaddingLength),
      props.getOptionalSet[String](PaddingFields),
      props.getEnumValue[PaddingType, PaddingType.type](PaddingType, PaddingSelection),
    )
}

case class DefaultPaddingService(
  fields:          Set[String],
  paddingStrategy: PaddingStrategy,
) extends PaddingService {

  def getPadderForField(field: String): String => String =
    if (fields.contains(field)) {
      paddingStrategy.padString
    } else {
      identity
    }
}

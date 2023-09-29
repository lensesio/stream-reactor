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
package io.lenses.streamreactor.connect.cloud.sink.config.padding

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import com.datamountaineer.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.sink.config.padding.PaddingService.DefaultPadChar
import io.lenses.streamreactor.connect.cloud.sink.config.padding.PaddingService.DefaultPadLength

/**
  * Retrieves Padding settings from KCQL
  */
trait PaddingStrategyConfigKeys extends WithConnectorPrefix {

  val PADDING_STRATEGY = s"$connectorPrefix.padding.strategy"
  val PADDING_STRATEGY_DOC =
    "Configure in order to pad the partition and offset on the sink output files. Options are `LeftPad`, `RightPad` or `NoOp`  (does not add padding). Defaults to `LeftPad`."
  val PADDING_STRATEGY_DEFAULT = ""

  val PADDING_LENGTH     = s"$connectorPrefix.padding.length"
  val PADDING_LENGTH_DOC = s"Length to pad the string up to if $PADDING_STRATEGY is set."
  val PADDING_LENGTH_DEFAULT: Int = -1

}

trait PaddingStrategySettings extends BaseSettings with PaddingStrategyConfigKeys {

  def getPaddingStrategy: Option[PaddingStrategy] = {
    val paddingLength = Option(getInt(PADDING_LENGTH)).filterNot(_ < 0).map(_.toInt)
    for {
      paddingType <- Option(getString(PADDING_STRATEGY)).filterNot(_ == "").flatMap(
        PaddingType.withNameInsensitiveOption,
      )
    } yield paddingType.toPaddingStrategy(
      paddingLength.getOrElse(DefaultPadLength),
      DefaultPadChar,
    )
  }
}

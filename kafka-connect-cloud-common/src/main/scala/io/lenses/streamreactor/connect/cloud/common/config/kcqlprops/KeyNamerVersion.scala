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
package io.lenses.streamreactor.connect.cloud.common.config.kcqlprops

import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.KeyNameFormatVersion
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

sealed trait KeyNamerVersion extends EnumEntry

object KeyNamerVersion extends Enum[KeyNamerVersion] {

  case object V0 extends KeyNamerVersion

  case object V1 extends KeyNamerVersion

  def apply(
    props:   KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
    default: KeyNamerVersion,
  ): KeyNamerVersion = fromProps(props).getOrElse(default)

  private def fromProps(props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Option[KeyNamerVersion] =
    props.getOptionalInt(KeyNameFormatVersion).collect {
      case 0 => V0
      case 1 => V1
    }

  override def values: IndexedSeq[KeyNamerVersion] = findValues
}

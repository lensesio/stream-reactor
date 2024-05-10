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
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings

sealed abstract class PropsKeyEntry(override val entryName: String) extends EnumEntry

object PropsKeyEnum extends Enum[PropsKeyEntry] {

  override val values: IndexedSeq[PropsKeyEntry] = findValues

  case object ReadTextMode extends PropsKeyEntry("read.text.mode")

  case object ReadRegex extends PropsKeyEntry("read.text.regex")

  case object ReadStartTag extends PropsKeyEntry("read.text.start.tag")

  case object ReadEndTag extends PropsKeyEntry("read.text.end.tag")
  case object BufferSize extends PropsKeyEntry("read.text.buffer.size")

  case object ReadStartLine extends PropsKeyEntry("read.text.start.line")
  case object ReadEndLine   extends PropsKeyEntry("read.text.end.line")

  case object ReadLastEndLineMissing extends PropsKeyEntry("read.text.last.end.line.missing")

  case object ReadTrimLine extends PropsKeyEntry("read.text.trim")

  case object StoreEnvelope         extends PropsKeyEntry(DataStorageSettings.StoreEnvelopeKey)
  case object StoreEnvelopeKey      extends PropsKeyEntry(DataStorageSettings.StoreKeyKey)
  case object StoreEnvelopeHeaders  extends PropsKeyEntry(DataStorageSettings.StoreHeadersKey)
  case object StoreEnvelopeValue    extends PropsKeyEntry(DataStorageSettings.StoreValueKey)
  case object StoreEnvelopeMetadata extends PropsKeyEntry(DataStorageSettings.StoreMetadataKey)

  case object PaddingLength extends PropsKeyEntry("padding.length")

  case object PaddingCharacter extends PropsKeyEntry("padding.char")

  case object PaddingSelection extends PropsKeyEntry("padding.type")

  case object PartitionIncludeKeys extends PropsKeyEntry("partition.include.keys")

  case object FlushSize extends PropsKeyEntry("flush.size")

  case object FlushCount extends PropsKeyEntry("flush.count")

  case object FlushInterval extends PropsKeyEntry("flush.interval")

}

/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PartitionIncludeKeys
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.SinkPropsSchema
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionDisplayTest extends AnyFlatSpec with MockitoSugar with Matchers with BeforeAndAfter {

  private val kcql: Kcql = mock[Kcql]
  private val emptyProps: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type] =
    KcqlProperties[PropsKeyEntry, PropsKeyEnum.type](schema = SinkPropsSchema.schema, map = Map.empty)

  before {
    reset(kcql)
  }

  "apply" should "recognise Keys from KCQL props" in {
    def keyValueProp(includeKeys: Boolean): KcqlProperties[PropsKeyEntry, PropsKeyEnum.type] =
      KcqlProperties[PropsKeyEntry, PropsKeyEnum.type](schema = SinkPropsSchema.schema,
                                                       map = Map(
                                                         PartitionIncludeKeys.entryName -> includeKeys.toString,
                                                       ),
      )
    PartitionDisplay(keyValueProp(true), Values) should be(KeysAndValues)
    PartitionDisplay(keyValueProp(false), Values) should be(Values)
  }

  "apply" should "default to specified default when no partitioner specified in kcql" in {
    PartitionDisplay(emptyProps, KeysAndValues) should be(KeysAndValues)
    PartitionDisplay(emptyProps, Values) should be(Values)
  }
}

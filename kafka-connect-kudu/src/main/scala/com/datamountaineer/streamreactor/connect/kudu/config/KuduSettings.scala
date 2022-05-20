/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.kudu.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.kcql.WriteModeEnum
import com.datamountaineer.streamreactor.common.errors.ErrorPolicy
import com.datamountaineer.streamreactor.common.errors.ThrowErrorPolicy

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class KuduSettings(
  kcql:                List[Kcql],
  topicTables:         Map[String, String],
  allowAutoCreate:     Map[String, Boolean],
  allowAutoEvolve:     Map[String, Boolean],
  fieldsMap:           Map[String, Map[String, String]],
  ignoreFields:        Map[String, Set[String]],
  writeModeMap:        Map[String, WriteModeEnum],
  errorPolicy:         ErrorPolicy = new ThrowErrorPolicy,
  maxRetries:          Int         = KuduConfigConstants.NBR_OF_RETIRES_DEFAULT,
  schemaRegistryUrl:   String,
  writeFlushMode:      WriteFlushMode.WriteFlushMode,
  mutationBufferSpace: Int,
)

object KuduSettings {

  def apply(config: KuduConfig): KuduSettings = {

    val kcql                = config.getKCQL
    val errorPolicy         = config.getErrorPolicy
    val maxRetries          = config.getNumberRetries
    val autoCreate          = config.getAutoCreate()
    val autoEvolve          = config.getAutoEvolve()
    val schemaRegUrl        = config.getSchemaRegistryUrl
    val fieldsMap           = config.getFieldsMap()
    val ignoreFields        = config.getIgnoreFieldsMap()
    val writeModeMap        = config.getWriteMode()
    val topicTables         = config.getTableTopic()
    val writeFlushMode      = config.getWriteFlushMode
    val mutationBufferSpace = config.getInt(KuduConfigConstants.MUTATION_BUFFER_SPACE)

    new KuduSettings(kcql                = kcql.toList,
                     topicTables         = topicTables,
                     allowAutoCreate     = autoCreate,
                     allowAutoEvolve     = autoEvolve,
                     fieldsMap           = fieldsMap,
                     ignoreFields        = ignoreFields,
                     writeModeMap        = writeModeMap,
                     errorPolicy         = errorPolicy,
                     maxRetries          = maxRetries,
                     schemaRegistryUrl   = schemaRegUrl,
                     writeFlushMode      = writeFlushMode,
                     mutationBufferSpace = mutationBufferSpace,
    )
  }
}

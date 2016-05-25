/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.hbase.config

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object HbaseSinkConfig {
  val COLUMN_FAMILY = "connect.hbase.sink.column.family"
  val COLUMN_FAMILY_DOC = "The hbase column family."

  val TABLE_NAME = "connect.hbase.sink.table.name"
  val TABLE_NAME_DOC = "The hbase table to store the data to"


  val ROW_KEYS = "connect.hbase.sink.key"
  val ROW_KEYS_DOC =
    """
      |Specifies which of the payload fields make up the Hbase row key. Multiple fields can be specified by separating them via a comma;
      | The fields are combined using a key separator by default is set to <\\n>.
    """.stripMargin

  val FIELDS = "connect.hbase.sink.fields"
  val FIELDS_DOC =
    """
      |Specifies which fields to consider when inserting the new Hbase entry. If is not set it will use insert all the payload fields present in the payload.
      |Field mapping is supported; this way an avro record field can be inserted into a 'mapped' column.
      |Examples:
      |* fields to be used:field1,field2,field3
      |** fields with mapping: field1=alias1,field2,field3=alias3"
    """.stripMargin

  val ROW_KEY_MODE = "connect.hbase.sink.rowkey.mode"
  val ROW_KEY_MODE_DOC =
    """
      |There are three available modes: SINK_RECORD, GENERIC and FIELDS.
      |SINK_RECORD - uses the SinkRecord.keyValue as the Hbase row key;
      |FIELDS - combines the specified payload fields to make up the Hbase row key
      |GENERIC- combines the kafka topic, offset and partition to build the Hbase row key."
      |
    """.stripMargin

  val config: ConfigDef = new ConfigDef()
    .define(COLUMN_FAMILY, Type.STRING, Importance.HIGH, COLUMN_FAMILY_DOC)
    .define(TABLE_NAME, Type.STRING, Importance.HIGH, TABLE_NAME_DOC)
    .define(ROW_KEY_MODE, Type.STRING, Importance.HIGH, ROW_KEY_MODE_DOC)
    .define(FIELDS, Type.STRING, Importance.LOW, FIELDS_DOC)
    .define(ROW_KEYS, Type.STRING, Importance.LOW, ROW_KEYS_DOC)
}

/**
  * <h1>HbaseSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class HbaseSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(HbaseSinkConfig.config, props)

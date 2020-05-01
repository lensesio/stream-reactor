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

package com.datamountaineer.streamreactor.connect.hbase.config

import com.datamountaineer.streamreactor.connect.hbase.{GenericRowKeyBuilderBytes, StructFieldsRowKeyBuilderBytes}
import org.apache.kafka.common.config.ConfigException
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class HBaseSettingsTest extends AnyWordSpec with Matchers with MockitoSugar {

  val TABLE_NAME_RAW = "someTable"
  val QUERY_ALL = s"INSERT INTO $TABLE_NAME_RAW SELECT * FROM $TABLE_NAME_RAW"
  val QUERY_ALL_KEYS = s"INSERT INTO $TABLE_NAME_RAW SELECT * FROM $TABLE_NAME_RAW PK lastName"
  val QUERY_SELECT = s"INSERT INTO $TABLE_NAME_RAW SELECT lastName as surname, firstName FROM $TABLE_NAME_RAW"
  val QUERY_SELECT_KEYS = s"INSERT INTO $TABLE_NAME_RAW SELECT lastName as surname, firstName FROM $TABLE_NAME_RAW " +
    s"PK surname"
  val QUERY_SELECT_KEYS_BAD: String = s"INSERT INTO $TABLE_NAME_RAW SELECT lastName as surname, firstName FROM $TABLE_NAME_RAW " +
    s"PK IamABadPersonAndIHateYou"

  "raise a configuration exception if the column family is empty" in {
    intercept[ConfigException] {
      val config = mock[HBaseConfig]
      when(config.getString(HBaseConfigConstants.COLUMN_FAMILY)).thenReturn("")
      when(config.getString(HBaseConfigConstants.ERROR_POLICY)).thenReturn("THROW")
      HBaseSettings(config)
    }
  }

  "correctly create a HbaseSettings when fields are row keys are provided" in {
    val props = Map(
      HBaseConfigConstants.KCQL_QUERY->QUERY_ALL_KEYS,
      HBaseConfigConstants.COLUMN_FAMILY->"somecolumnFamily"
    ).asJava

    val config = HBaseConfig(props)
    val settings = HBaseSettings(config)
    val kcql = settings.routes.head
    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[StructFieldsRowKeyBuilderBytes] shouldBe true

    kcql.getFields.asScala.head.getName shouldBe "*"
    kcql.getTarget shouldBe TABLE_NAME_RAW
    kcql.getSource shouldBe TABLE_NAME_RAW
  }

  "correctly create a HbaseSettings when no row fields are provided" in {
    val props = Map(
      HBaseConfigConstants.KCQL_QUERY->QUERY_ALL,
      HBaseConfigConstants.COLUMN_FAMILY->"somecolumnFamily"
    ).asJava

    val config = HBaseConfig(props)
    val settings = HBaseSettings(config)
    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[GenericRowKeyBuilderBytes] shouldBe true
    val kcql = settings.routes.head

    kcql.getFields.asScala.head.getName shouldBe "*"
    kcql.getSource shouldBe TABLE_NAME_RAW
    kcql.getTarget shouldBe TABLE_NAME_RAW
  }

  "correctly create a HbaseSettings when no row fields are provided and selection" in {

    val props = Map(
      HBaseConfigConstants.KCQL_QUERY->QUERY_SELECT,
      HBaseConfigConstants.COLUMN_FAMILY->"somecolumnFamily"
    ).asJava

    val config = HBaseConfig(props)
    val settings = HBaseSettings(config)
    val kcql = settings.routes.head
    val fields = kcql.getFields.asScala.toList

    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[GenericRowKeyBuilderBytes] shouldBe true

    kcql.getFields.asScala.head.getName.equals("*") shouldBe false
    kcql.getSource shouldBe TABLE_NAME_RAW
    kcql.getTarget shouldBe TABLE_NAME_RAW
    fields.head.getName shouldBe "lastName"
    fields.head.getAlias shouldBe "surname"
    fields.last.getName shouldBe "firstName"
    fields.last.getAlias shouldBe "firstName"
  }

  "correctly create a HbaseSettings when row fields are provided and selection" in {
    val props = Map(
      HBaseConfigConstants.KCQL_QUERY->QUERY_SELECT_KEYS,
      HBaseConfigConstants.COLUMN_FAMILY->"somecolumnFamily"
    ).asJava

    val config = HBaseConfig(props)
    val settings = HBaseSettings(config)
    val kcql = settings.routes.head
    val fields = kcql.getFields.asScala.toList

    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[StructFieldsRowKeyBuilderBytes] shouldBe true

    kcql.getFields.asScala.head.getName.equals("*") shouldBe false
    kcql.getSource shouldBe TABLE_NAME_RAW
    kcql.getTarget shouldBe TABLE_NAME_RAW
    fields.head.getName shouldBe "lastName"
    fields.head.getAlias shouldBe "surname"
    fields.last.getName shouldBe "firstName"
    fields.last.getAlias shouldBe "firstName"
  }

  //  "raise an exception when the row key builder is set to FIELDS but pks not in query map" in {
  //    intercept[java.lang.IllegalArgumentException] {
  //      val config = mock[HbaseSinkConfig]
  //      val columnFamily = "somecolumnFamily"
  //      when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_SELECT_KEYS_BAD) //set keys in select
  //      when(config.getString(COLUMN_FAMILY)).thenReturn(columnFamily)
  //      when(config.getString(HbaseSinkConfig.ERROR_POLICY)).thenReturn("THROW")
  //      HbaseSettings(config)
  //    }
  //  }
}

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
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class HbaseSettingsTest extends WordSpec with Matchers with MockitoSugar {

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
      val config = mock[HbaseSinkConfig]
      when(config.getString(HbaseSinkConfigConstants.COLUMN_FAMILY)).thenReturn("")
      when(config.getString(HbaseSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")
      HbaseSettings(config)
    }
  }

  "correctly create a HbaseSettings when fields are row keys are provided" in {
    val config = mock[HbaseSinkConfig]
    val columnFamily = "somecolumnFamily"

    when(config.getString(HbaseSinkConfigConstants.COLUMN_FAMILY)).thenReturn(columnFamily)
    when(config.getString(HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY)).thenReturn(QUERY_ALL_KEYS)
    when(config.getString(HbaseSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")

    val settings = HbaseSettings(config)
    val route = settings.routes.head

    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[StructFieldsRowKeyBuilderBytes] shouldBe true

    route.isIncludeAllFields shouldBe true
    route.getTarget shouldBe TABLE_NAME_RAW
    route.getSource shouldBe TABLE_NAME_RAW
  }

  "correctly create a HbaseSettings when no row fields are provided" in {
    val config = mock[HbaseSinkConfig]
    val columnFamily = "somecolumnFamily"

    when(config.getString(HbaseSinkConfigConstants.COLUMN_FAMILY)).thenReturn(columnFamily)
    when(config.getString(HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY)).thenReturn(QUERY_ALL)
    when(config.getString(HbaseSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")

    val settings = HbaseSettings(config)

    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[GenericRowKeyBuilderBytes] shouldBe true
    val route = settings.routes.head

    route.isIncludeAllFields shouldBe true
    route.getSource shouldBe TABLE_NAME_RAW
    route.getTarget shouldBe TABLE_NAME_RAW
  }

  "correctly create a HbaseSettings when no row fields are provided and selection" in {
    val config = mock[HbaseSinkConfig]
    val columnFamily = "somecolumnFamily"

    when(config.getString(HbaseSinkConfigConstants.COLUMN_FAMILY)).thenReturn(columnFamily)
    when(config.getString(HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY)).thenReturn(QUERY_SELECT)
    when(config.getString(HbaseSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")

    val settings = HbaseSettings(config)
    val route = settings.routes.head
    val fields = route.getFieldAlias.asScala.toList

    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[GenericRowKeyBuilderBytes] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe TABLE_NAME_RAW
    route.getTarget shouldBe TABLE_NAME_RAW
    fields.head.getField shouldBe "lastName"
    fields.head.getAlias shouldBe "surname"
    fields.last.getField shouldBe "firstName"
    fields.last.getAlias shouldBe "firstName"
  }

  "correctly create a HbaseSettings when row fields are provided and selection" in {
    val config = mock[HbaseSinkConfig]
    val columnFamily = "somecolumnFamily"

    when(config.getString(HbaseSinkConfigConstants.COLUMN_FAMILY)).thenReturn(columnFamily)
    when(config.getString(HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY)).thenReturn(QUERY_SELECT_KEYS)
    when(config.getString(HbaseSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")

    val settings = HbaseSettings(config)
    val route = settings.routes.head
    val fields = route.getFieldAlias.asScala.toList

    settings.rowKeyModeMap(TABLE_NAME_RAW).isInstanceOf[StructFieldsRowKeyBuilderBytes] shouldBe true

    route.isIncludeAllFields shouldBe false
    route.getSource shouldBe TABLE_NAME_RAW
    route.getTarget shouldBe TABLE_NAME_RAW
    fields.head.getField shouldBe "lastName"
    fields.head.getAlias shouldBe "surname"
    fields.last.getField shouldBe "firstName"
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

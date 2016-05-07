package com.datamountaineer.streamreactor.connect.hbase.config

import com.datamountaineer.streamreactor.connect.hbase.config.HbaseSinkConfig._
import org.apache.kafka.common.config.ConfigException
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class HbaseSettingsTest extends WordSpec with Matchers with MockitoSugar {

  "HbaseSettings" should {
    "raise a configuration exception if table name is empty" in {
      intercept[ConfigException] {
        val config = mock[HbaseSinkConfig]
        when(config.getString(TABLE_NAME)).thenReturn("")
        when(config.getString(COLUMN_FAMILY)).thenReturn("somecolumnfamily")
        when(config.getString(ROW_KEY_MODE)).thenReturn("DEFAULT")
        HbaseSettings(config)
      }
    }

    "raise a configuration exception if the column family is empty" in {
      intercept[ConfigException] {
        val config = mock[HbaseSinkConfig]
        when(config.getString(TABLE_NAME)).thenReturn("sometable")
        when(config.getString(COLUMN_FAMILY)).thenReturn("")
        when(config.getString(ROW_KEY_MODE)).thenReturn("DEFAULT")
        HbaseSettings(config)
      }
    }

    "correctly create a HbaseSettings when no fields are provided" in {
      val config = mock[HbaseSinkConfig]
      val tableName = "sometable"
      val columnFamily = "somecolumnFamily"
      val rowKeyMode = "FIELDS"

      when(config.getString(TABLE_NAME)).thenReturn(tableName)
      when(config.getString(COLUMN_FAMILY)).thenReturn(columnFamily)
      when(config.getString(ROW_KEY_MODE)).thenReturn(rowKeyMode)
      when(config.getString(ROW_KEYS)).thenReturn("firstName,lastName")

      val settings = HbaseSettings(config)

      settings shouldBe HbaseSettings(tableName,
        columnFamily,
        HbaseRowKey(rowKeyMode, Seq("firstName", "lastName")),
        HbaseFields(true, Map.empty))
    }

    "correctly create a HbaseSettings when fields/column mappings are provided with no alias" in {
      val config = mock[HbaseSinkConfig]
      val tableName = "sometable"
      val columnFamily = "somecolumnFamily"
      val rowKeyMode = "FIELDS"

      when(config.getString(TABLE_NAME)).thenReturn(tableName)
      when(config.getString(COLUMN_FAMILY)).thenReturn(columnFamily)
      when(config.getString(ROW_KEY_MODE)).thenReturn(rowKeyMode)
      when(config.getString(FIELDS)).thenReturn("age,firstName, lastName")
      when(config.getString(ROW_KEYS)).thenReturn("firstName,lastName")

      val settings = HbaseSettings(config)

      settings shouldBe HbaseSettings(tableName,
        columnFamily,
        HbaseRowKey(rowKeyMode, Seq("firstName", "lastName")),
        HbaseFields(false, Map("firstName" -> "firstName",
          "lastName" -> "lastName",
          "age" -> "age")))
    }

    "correctly create a HbaseSettings when column mappings are provided" in {
      val config = mock[HbaseSinkConfig]
      val tableName = "sometable"
      val columnFamily = "somecolumnFamily"
      val rowKeyMode = "FIELDS"


      when(config.getString(TABLE_NAME)).thenReturn(tableName)
      when(config.getString(COLUMN_FAMILY)).thenReturn(columnFamily)
      when(config.getString(ROW_KEY_MODE)).thenReturn(rowKeyMode)
      when(config.getString(ROW_KEYS)).thenReturn("lastName,age")
      when(config.getString(FIELDS)).thenReturn("age, firstName, lastName=Name")

      val settings = HbaseSettings(config)

      settings shouldBe HbaseSettings(tableName,
        columnFamily,
        HbaseRowKey(rowKeyMode, Seq("lastName", "age")),
        HbaseFields(false, Map("firstName" -> "firstName",
          "lastName" -> "Name",
          "age" -> "age")))
    }

  }

  "raise an exception when the row key builder is set to FIELDS but fields are not specified" in {
    intercept[ConfigException] {
      val config = mock[HbaseSinkConfig]
      val tableName = "sometable"
      val columnFamily = "somecolumnFamily"
      val rowKeyMode = "FIELDS"


      when(config.getString(TABLE_NAME)).thenReturn(tableName)
      when(config.getString(COLUMN_FAMILY)).thenReturn(columnFamily)
      when(config.getString(ROW_KEY_MODE)).thenReturn(rowKeyMode)
      HbaseSettings(config)

    }
  }
}

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

import com.datamountaineer.streamreactor.connect.hbase.config.HbaseSinkConfig._
import org.apache.kafka.common.config.ConfigException
import scala.util.{Failure, Success, Try}

case class HbaseSettings(tableName: String,
                         columnFamily: String,
                         rowKey: HbaseRowKey,
                         fields: HbaseFields)

case class HbaseRowKey(mode: String, keys: Seq[String])

case class HbaseFields(includeAllFields: Boolean,
                       fieldsMappings: Map[String, String])


object HbaseSettings {

  val FieldsRowKeyMode = "FIELDS"
  val SinkRecordRowKeyMode = "SINK_RECORD"
  val GenericRowKeyMode = "GENERIC"


  /**
    * Creates an instance of HbaseSettings from a HbaseSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of HbaseSettings
    */
  def apply(config: HbaseSinkConfig): HbaseSettings = {
    val tableName = config.getString(TABLE_NAME)
    if (tableName.trim.length == 0) {
      throw new ConfigException(s"$TABLE_NAME is not set correctly.")
    }

    val columnFamily = config.getString(COLUMN_FAMILY)
    if (columnFamily.trim.length == 0) {
      throw new ConfigException(s"$COLUMN_FAMILY is not set correctly")
    }

    val rowKeyMode = config.getString(ROW_KEY_MODE)
    val allRowKeyModes = Set(FieldsRowKeyMode, SinkRecordRowKeyMode, GenericRowKeyMode)
    if (!allRowKeyModes.contains(rowKeyMode)) {
      throw new ConfigException(s"$rowKeyMode is not recognized. Available modes are ${allRowKeyModes.mkString(",")}.")
    }

    HbaseSettings(tableName,
      columnFamily,
      HbaseRowKey(rowKeyMode, getRowKeys(rowKeyMode, config)),
      HbaseFields(Try(config.getString(FIELDS)).toOption.flatMap(v => Option(v))))

  }

  /**
    * Returns a sequence of fields making the Hbase row key.
    *
    * @param rowKeyMode RowKeyMode  FIELDS, SINK_RECORD, GENERIC
    * @param config HBaseSinkConfig for the connector
    * @return A sequence of fields if the row key mode is set to FIELDS, empty otherwise
    */
  private def getRowKeys(rowKeyMode: String, config: HbaseSinkConfig): Seq[String] = {
    if (rowKeyMode == FieldsRowKeyMode) {
      val rowKeys = Try(config.getString(ROW_KEYS)) match {
        case Failure(t) => Seq.empty[String]
        case Success(null) => Seq.empty[String]
        case Success(value) => value.split(",").map(_.trim).toSeq
      }
      if (rowKeys.isEmpty) {
        throw new ConfigException("Fields defining the row key are required.")
      }

      rowKeys
    }
    else {
      Seq.empty
    }
  }

}

object HbaseFields {
  /**
    * Works out the fields and their mappings to be used when inserting a new Hbase row
    *
    * @param setting - The configuration specifing the fields and their mappings
    * @return A dictionary of fields and their mappings alongside a flag specifying if all fields should be used.
    *         If no mapping has been specified the field name is considered to be the mapping
    */
  def apply(setting: Option[String]): HbaseFields = {
    setting match {
      case None =>
        HbaseFields(includeAllFields = true, Map.empty[String, String])
      case Some(c) =>

        val mappings = c.split(",").map { case f =>
          f.trim.split("=").toSeq match {
            case Seq(field) =>
              field -> field
            case Seq(field, alias) =>
              field -> alias
            case _ => throw new ConfigException(s"$c is not valid. Need to set the fields and mappings like: " +
              s"field1,field2,field3=alias3,[field4, field5=alias5]")
          }
        }.toMap

        HbaseFields(mappings.contains("*"), mappings - "*")
    }
  }
}

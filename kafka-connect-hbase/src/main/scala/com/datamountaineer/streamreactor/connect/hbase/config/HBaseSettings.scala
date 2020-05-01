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

import java.io.File

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.hbase.config.HBaseConfigConstants._
import com.datamountaineer.streamreactor.connect.hbase.kerberos.Kerberos
import com.datamountaineer.streamreactor.connect.hbase.{GenericRowKeyBuilderBytes, RowKeyBuilderBytes, StructFieldsExtractorBytes, StructFieldsRowKeyBuilderBytes}
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._

case class HBaseSettings(columnFamilyMap: String,
                         rowKeyModeMap: Map[String, RowKeyBuilderBytes],
                         routes: List[Kcql],
                         extractorFields: Map[String, StructFieldsExtractorBytes],
                         errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                         maxRetries: Int = HBaseConfigConstants.NBR_OF_RETIRES_DEFAULT,
                         hbaseConfigDir: Option[String],
                         kerberos: Option[Kerberos]
                        )

object HBaseSettings {

  /**
    * Creates an instance of HbaseSettings from a HbaseSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of HbaseSettings
    */
  def apply(config: HBaseConfig): HBaseSettings = {
    val columnFamily = config.getString(COLUMN_FAMILY)

    if (columnFamily.trim.length == 0) throw new ConfigException(s"$COLUMN_FAMILY is not set correctly")

    val kcql = config.getKCQL
    val fields = config.getFieldsMap()
    val errorPolicy = config.getErrorPolicy
    val nbrOfRetries = config.getNumberRetries

    val rowKeyModeMap = kcql.map(r => {
      val keys = r.getPrimaryKeys.asScala.map(p => p.getName).toList
      if (keys.nonEmpty) (r.getSource, StructFieldsRowKeyBuilderBytes(keys)) else (r.getSource, new GenericRowKeyBuilderBytes())
    }
    ).toMap

    val extractorFields = kcql.map(rm => {
      val allFields = rm.getFields.asScala.exists(_.getName.equals("*"))
      (rm.getSource, StructFieldsExtractorBytes(allFields, fields(rm.getSource)))
    }).toMap

    val hbaseConfigDir = Option(config.getString(HBaseConfigConstants.HBASE_CONFIG_DIR))

    def validate(dir: String, key: String): Unit = {
      val folder = new File(dir)
      if (!folder.exists() || !folder.isDirectory) {
        throw new ConfigException(s"Invalid configuration for [$key]. Folder can not be found")
      }
    }
    hbaseConfigDir.foreach(validate(_, HBaseConfigConstants.HBASE_CONFIG_DIR))

    new HBaseSettings(columnFamily, rowKeyModeMap, kcql.toList, extractorFields,
      errorPolicy, nbrOfRetries, hbaseConfigDir, Kerberos.from(config, HBaseConfigConstants))
  }
}

/**
  * Copyright 2015 Datamountaineer.
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

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.hbase.config.HbaseSinkConfig._
import com.datamountaineer.streamreactor.connect.rowkeys._
import org.apache.kafka.common.config.ConfigException
import scala.collection.JavaConverters._

case class HbaseSettings(columnFamilyMap: String,
                         rowKeyModeMap : Map[String, RowKeyBuilderBytes],
                         routes: List[Config],
                         errorPolicy : ErrorPolicy = new ThrowErrorPolicy,
                         maxRetries : Int = HbaseSinkConfig.NBR_OF_RETIRES_DEFAULT
                        )


object HbaseSettings {

  /**
    * Creates an instance of HbaseSettings from a HbaseSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of HbaseSettings
    */
  def apply(config: HbaseSinkConfig, assigned : List[String]): HbaseSettings = {

    val columnFamily = config.getString(COLUMN_FAMILY)
    if (columnFamily.trim.length == 0) {
      throw new ConfigException(s"$COLUMN_FAMILY is not set correctly")
    }

    val raw = config.getString(HbaseSinkConfig.EXPORT_ROUTE_QUERY)
    require((raw != null && !raw.isEmpty),  s"No ${HbaseSinkConfig.EXPORT_ROUTE_QUERY} provided!")

    //parse query
    val routes: Set[Config] = raw.split(";").map(r => Config.parse(r)).toSet.filter(f=>assigned.contains(f.getSource))

    if (routes.size == 0) {
      throw new ConfigException(s"No routes for for assigned topics in "
        + s"${HbaseSinkConfig.EXPORT_ROUTE_QUERY}")
    }

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(HbaseSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val nbrOfRetries = config.getInt(HbaseSinkConfig.NBR_OF_RETRIES)

    val rowKeyModeMap = routes.map(
      r=> {
        val keys = r.getPrimaryKeys.asScala.toList

        if (keys.size != 0) {
          (r.getSource, StructFieldsRowKeyBuilderBytes(keys))
        } else {
          (r.getSource, new GenericRowKeyBuilderBytes())
        }

      }
    ).toMap

    new HbaseSettings(columnFamily, rowKeyModeMap, routes.toList, errorPolicy, nbrOfRetries)
  }
}

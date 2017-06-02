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

package com.datamountaineer.streamreactor.connect.elastic.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum}
import org.elasticsearch.plugins.Plugin

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * stream-reactor-maven
  */
case class ElasticSettings(routes: List[Config],
                           fields: Map[String, Map[String, String]],
                           ignoreFields: Map[String, Set[String]],
                           pks: Map[String, String],
                           tableMap: Map[String, String],
                           errorPolicy: ErrorPolicy,
                           taskRetries: Int = ElasticSinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
                           writeTimeout: Int = ElasticSinkConfigConstants.WRITE_TIMEOUT_DEFAULT,
                           xpackSettings: Map[String, String] = Map.empty,
                           xpackPluggins: Seq[Class[_ <: Plugin]] = Seq.empty)


object ElasticSettings {

  def apply(config: ElasticSinkConfig): ElasticSettings = {
    val raw = config.getString(ElasticSinkConfigConstants.KCQL)
    require(!raw.isEmpty, s"Empty ${ElasticSinkConfigConstants.KCQL_DOC}")
    val routes = raw.split(";").map(r => Config.parse(r)).toList

    val fields = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap

    val tableMap = routes.map(rm => (rm.getSource, rm.getTarget)).toMap
    val ignoreFields = routes.map(rm => (rm.getSource, rm.getIgnoredField.toSet)).toMap

    val pks = routes
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.toSet
        require(keys.nonEmpty && keys.size == 1, s"${r.getTarget} is set up with upsert, you need one primary key setup")
        (r.getSource, keys.head)
      }.toMap

    val writeTimeout = config.getInt(ElasticSinkConfigConstants.WRITE_TIMEOUT_CONFIG)

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(ElasticSinkConfigConstants.ERROR_POLICY_CONFIG).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val retries = config.getInt(ElasticSinkConfigConstants.NBR_OF_RETRIES_CONFIG)

    val xpackSettings = Option(config.getString(ElasticSinkConfigConstants.ES_CLUSTER_XPACK_SETTINGS))
      .map { value =>
        value.split(";").map { s =>
          s.split("=") match {
            case Array(k, v) => k -> v
            case _ => throw new IllegalArgumentException(s"Invalid setting provided for ${ElasticSinkConfigConstants.ES_CLUSTER_XPACK_SETTINGS}. '$s' is not a valid xpack setting. You need to provide in the format of 'key=value'")
          }
        }.toMap
      }.getOrElse(Map.empty)

    val xpackPlugins = Option(config.getString(ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS))
      .map { value =>
        val pluginClass = classOf[Plugin]
        value.split(";")
          .map { className =>
            val clz = Try {
              Class.forName(className)
            }.getOrElse(throw new IllegalArgumentException(s"Invalid setting provided for ${ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS}. Class '$value' can't be loaded "))
            if (!pluginClass.isAssignableFrom(clz)) {
              throw new IllegalArgumentException(s"Invalid setting provided for ${ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS}. Class '$value' is not derived from ${pluginClass.getCanonicalName}")
            }
            clz.asInstanceOf[Class[_ <: Plugin]]
          }
          .toSeq
      }.getOrElse(Seq.empty)

    ElasticSettings(routes = routes,
      fields = fields,
      ignoreFields = ignoreFields,
      pks = pks,
      tableMap = tableMap,
      errorPolicy,
      retries,
      writeTimeout,
      xpackSettings,
      xpackPlugins
    )
  }
}

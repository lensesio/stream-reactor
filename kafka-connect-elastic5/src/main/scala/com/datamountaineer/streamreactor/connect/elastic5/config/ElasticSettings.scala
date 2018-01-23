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

package com.datamountaineer.streamreactor.connect.elastic5.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic5.config.ClientType.ClientType
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.elasticsearch.plugins.Plugin

import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * stream-reactor-maven
  */
case class ElasticSettings(kcqls: Seq[Kcql],
                           errorPolicy: ErrorPolicy,
                           taskRetries: Int = ElasticConfigConstants.NBR_OF_RETIRES_DEFAULT,
                           writeTimeout: Int = ElasticConfigConstants.WRITE_TIMEOUT_DEFAULT,
                           xPackSettings: Map[String, String] = Map.empty,
                           xPackPlugins: Seq[Class[_ <: Plugin]] = Seq.empty,
                           clientType: ClientType = ClientType.TCP,
                           batchSize: Int = ElasticConfigConstants.BATCH_SIZE_DEFAULT,
                           pkJoinerSeparator: String = ElasticConfigConstants.PK_JOINER_SEPARATOR_DEFAULT
                          )


object ElasticSettings {

  def apply(config: ElasticConfig): ElasticSettings = {
    val kcql = config.getKcql()
    val pkJoinerSeparator = config.getString(ElasticConfigConstants.PK_JOINER_SEPARATOR)
    val writeTimeout = config.getWriteTimeout
    val errorPolicy = config.getErrorPolicy
    val retries = config.getNumberRetries
    val clientType = ClientType.withName(config.getString(ElasticConfigConstants.CLIENT_TYPE_CONFIG).toUpperCase)
    val rawXPack = Option(config.getPassword(ElasticConfigConstants.ES_CLUSTER_XPACK_SETTINGS))

    val xPackSettings = rawXPack
      .map { password =>
        password.value().split(";").map { s =>
          s.split("=") match {
            case Array(k, v) => k -> v
            case _ => throw new IllegalArgumentException(s"Invalid setting provided for ${ElasticConfigConstants.ES_CLUSTER_XPACK_SETTINGS}. '$s' is not a valid XPACK setting. You need to provide in the format of 'key=value'")
          }
        }.toMap
      }.getOrElse(Map.empty)

    if (xPackSettings.nonEmpty && clientType.equals(ClientType.HTTP)) {
      throw new ConfigException("XPACK can not be used with the HTTP client for elastic4s")
    }

    val xPackPlugins = Option(config.getString(ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS))
      .map { value =>
        val pluginClass = classOf[Plugin]
        value.split(";")
          .map { className =>
            val clz = Try {
              Class.forName(className)
            }.getOrElse(throw new IllegalArgumentException(s"Invalid setting provided for ${ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS}. Class '$value' can't be loaded "))
            if (!pluginClass.isAssignableFrom(clz)) {
              throw new IllegalArgumentException(s"Invalid setting provided for ${ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS}. Class '$value' is not derived from ${pluginClass.getCanonicalName}")
            }
            clz.asInstanceOf[Class[_ <: Plugin]]
          }
          .toSeq
      }.getOrElse(Seq.empty)


    val batchSize = config.getInt(ElasticConfigConstants.BATCH_SIZE_CONFIG)

    ElasticSettings(kcql,
      errorPolicy,
      retries,
      writeTimeout,
      xPackSettings,
      xPackPlugins,
      clientType,
      batchSize,
      pkJoinerSeparator
    )
  }
}

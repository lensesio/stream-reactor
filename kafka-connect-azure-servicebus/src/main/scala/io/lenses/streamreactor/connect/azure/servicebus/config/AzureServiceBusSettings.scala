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

package io.lenses.streamreactor.connect.azure.servicebus.config

import com.datamountaineer.kcql.Field
import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicy
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.servicebus.TargetType
import io.lenses.streamreactor.connect.azure.servicebus.config.AbstractConfigExtensions._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class AzureServiceBusSettings(
                                    sapName: String,
                                    sapKey: Password,
                                    namespace: String,
                                    pollInterval: Long,
                                    sessions: Map[String, String],
                                    targets: Map[String, String],
                                    fieldsMap: Map[String, Seq[Field]],
                                    partitionBy: Map[String, Set[String]],
                                    keys: Map[String, Seq[String]],
                                    delimiters: Map[String, String],
                                    batchSize: Map[String, Int],
                                    targetType: Map[String, TargetType.Value],
                                    autoCreate: Map[String, Boolean],
                                    errorPolicy: ErrorPolicy,
                                    maxRetries: Int = AzureServiceBusConfig.NBR_OF_RETIRES_DEFAULT,
                                    converters: Map[String, String],
                                    ttl: Map[String, Long],
                                    subscriptions: Map[String, String],
                                    setHeaders: Boolean

)

object AzureServiceBusSettings extends StrictLogging {
  def apply(config: AzureServiceBusConfig): AzureServiceBusSettings = {


    config.getRequiredString(AzureServiceBusConfig.AZURE_SAP_NAME)
    config.getRequiredPassword(AzureServiceBusConfig.AZURE_SAP_KEY)

    val namespace =
      config.getRequiredString(AzureServiceBusConfig.AZURE_SB_NAMESPACE)

    if (namespace.isEmpty) {
      throw new ConnectException(
        s"[${AzureServiceBusConfig.AZURE_SB_NAMESPACE}] not set")
    }

    val kcqls = config.getKCQL
    val fields = config.getFields(kcqls)
    val targets = config.getTableTopic(kcqls)

    val partitionBy =
      kcqls
        .filterNot(k => k.getPartitionBy.asScala.isEmpty)
        .map(k => (k.getSource, k.getPartitionBy.asScala.toSet))
        .toMap

    val errorPolicy = config.getErrorPolicy
    val maxRetries = config.getNumberRetries

    val batchSize = kcqls.toList
      .map(r =>
        (r.getSource, if (r.getBatchSize.equals(0)) AzureServiceBusConfig.DEFAULT_BATCH_SIZE else r.getBatchSize))
      .toMap

    val targetType =
      kcqls.toList
        .map(k => {
          val storeAs = Option(k.getStoredAs) match {
            case Some(value) => TargetType.withName(value.toUpperCase)
            case None =>
              logger.warn("[STOREAS] not set. Defaulting to [TOPIC]")
              TargetType.TOPIC
          }
          k.getSource -> storeAs
        })
        .toMap

    val converters = kcqls
      .map(k => {
        (k.getSource,
         if (k.getWithConverter == null)
           classOf[BytesConverter].getCanonicalName
         else k.getWithConverter)
      })
      .toMap

    converters.values.foreach(clazz =>
      Try(Class.forName(clazz)) match {
        case Failure(_) =>
          throw new ConfigException(
            s"Invalid [${AzureServiceBusConfig.KCQL}]. [$clazz] not found")
        case Success(clz) =>
          if (!classOf[Converter].isAssignableFrom(clz)) {
            throw new ConfigException(
              s"Invalid [${AzureServiceBusConfig.KCQL}]. [$clazz] is not inheriting Converter")
          }
    })

    val keys = kcqls
      .map(
        k =>
          k.getSource -> Option(k.getWithKeys)
            .map(l => l.asScala)
            .getOrElse(Seq.empty))
      .toMap

    val delimiters = kcqls.map(k => (k.getSource, k.getKeyDelimeter)).toMap
    val ttl = config.getTTL().filter({ case (_, ttl) => ttl > 0 })
    val autoCreate = config.getAutoCreate()
    val sessions = kcqls
      .filterNot(k => Option(k.getWithSession).isEmpty)
      .map(k => (k.getSource, k.getWithSession))
      .toMap

    val subscriptions = kcqls.map(k => (k.getSource, k.getWithSubscription)).toMap
    val setHeaders = config.getBoolean(AzureServiceBusConfig.SET_HEADERS)

    AzureServiceBusSettings(
      sapName = config.getString(AzureServiceBusConfig.AZURE_SAP_NAME),
      sapKey = config.getPassword(AzureServiceBusConfig.AZURE_SAP_KEY),
      namespace = namespace,
      pollInterval = config.getLong(AzureServiceBusConfig.AZURE_POLL_INTERVAL),
      sessions = sessions,
      targets = targets,
      fieldsMap = fields,
      partitionBy = partitionBy,
      keys = keys,
      delimiters = delimiters,
      batchSize = batchSize,
      targetType = targetType,
      autoCreate = autoCreate,
      errorPolicy = errorPolicy,
      maxRetries = maxRetries,
      converters = converters,
      ttl = ttl,
      subscriptions = subscriptions,
      setHeaders = setHeaders
    )
  }
}

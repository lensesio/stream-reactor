package com.datamountaineer.streamreactor.connect.hazelcast.config

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
case class HazelCastSinkSettings(groupName : String,
                                 connConfig: HazelCastConnectionConfig,
                                 routes: Set[Config],
                                 topicObject: Map[String, String],
                                 fieldsMap : Map[String, Map[String, String]],
                                 ignoreFields: Map[String, Set[String]],
                                 errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                                 maxRetries: Int = HazelCastSinkConfig.NBR_OF_RETIRES_DEFAULT,
                                 batchSize: Int = HazelCastSinkConfig.BATCH_SIZE_DEFAULT)

object HazelCastSinkSettings {
  def apply(config: HazelCastSinkConfig): HazelCastSinkSettings = {
    val raw = config.getString(HazelCastSinkConfig.EXPORT_ROUTE_QUERY)
    require(raw.nonEmpty,  s"No ${HazelCastSinkConfig.EXPORT_ROUTE_QUERY} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(HazelCastSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val maxRetries = config.getInt(HazelCastSinkConfig.NBR_OF_RETRIES)
    val batchSize = config.getInt(HazelCastSinkConfig.BATCH_SIZE)
    val topicTables = routes.map(r => (r.getSource, r.getTarget)).toMap

    val fieldsMap = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField,fa.getAlias)).toMap)
    ).toMap

    val ignoreFields = routes.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap
    val groupName = config.getString(HazelCastSinkConfig.SINK_GROUP_NAME)
    require(groupName.nonEmpty,  s"No ${HazelCastSinkConfig.SINK_GROUP_NAME} provided!")
    val connConfig = HazelCastConnectionConfig(config)

    new HazelCastSinkSettings(groupName,
                              connConfig,
                              routes,
                              topicTables,
                              fieldsMap,
                              ignoreFields,
                              errorPolicy,
                              maxRetries,
                              batchSize)
  }
}



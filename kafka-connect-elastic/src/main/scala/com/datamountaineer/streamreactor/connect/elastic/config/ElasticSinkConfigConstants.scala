package com.datamountaineer.streamreactor.connect.elastic.config

/**
  * Created by bento on 10/04/2017.
  */
object ElasticSinkConfigConstants {

  val URL = "connect.elastic.url"
  val URL_DOC = "Url including port for Elastic Search cluster node."
  val URL_DEFAULT = "localhost:9300"
  val ES_CLUSTER_NAME = "connect.elastic.cluster.name"
  val ES_CLUSTER_NAME_DEFAULT = "elasticsearch"
  val ES_CLUSTER_NAME_DOC = "Name of the elastic search cluster, used in local mode for setting the connection"
  val URL_PREFIX = "connect.elastic.url.prefix"
  val URL_PREFIX_DOC = "URL connection string prefix"
  val URL_PREFIX_DEFAULT = "elasticsearch"
  val EXPORT_ROUTE_QUERY = "connect.elastic.sink.kcql"
  val EXPORT_ROUTE_QUERY_DOC = "KCQL expression describing field selection and routes."
}

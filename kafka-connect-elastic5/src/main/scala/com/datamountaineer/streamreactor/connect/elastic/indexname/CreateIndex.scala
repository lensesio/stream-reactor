package com.datamountaineer.streamreactor.connect.elastic.indexname

import com.datamountaineer.connector.config.Config

/**
  * Creates the index for the given KCQL configuration.
  */
object CreateIndex {
  def getIndexName(config: Config): String = {
    Option(config.getIndexSuffix).fold(config.getTarget) { indexNameSuffix =>
      s"${config.getTarget}${CustomIndexName.parseIndexName(indexNameSuffix)}"
    }
  }
}

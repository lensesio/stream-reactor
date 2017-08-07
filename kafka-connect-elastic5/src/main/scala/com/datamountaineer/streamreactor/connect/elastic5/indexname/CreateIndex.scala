package com.datamountaineer.streamreactor.connect.elastic.indexname

import com.datamountaineer.kcql.Kcql

/**
  * Creates the index for the given KCQL configuration.
  */
object CreateIndex {
  def getIndexName(kcql: Kcql): String = {
    Option(kcql.getIndexSuffix).fold(kcql.getTarget) { indexNameSuffix =>
      s"${kcql.getTarget}${CustomIndexName.parseIndexName(indexNameSuffix)}"
    }
  }
}

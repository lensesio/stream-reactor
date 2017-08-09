package com.datamountaineer.streamreactor.connect.elastic5.indexname

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic5.indexname.CustomIndexName

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

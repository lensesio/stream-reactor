package com.datamountaineer.streamreactor.connect.elastic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.datamountaineer.connector.config.Config
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic.indexname.CreateIndex
import org.scalatest.{Matchers, WordSpec}

class CreateIndexTest extends WordSpec with Matchers {
  "CreateIndex" should {
    "create an index name without suffix when suffix not set" in {
      val kcql = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA")
      CreateIndex.getIndexName(kcql) shouldBe "index_name"
    }

    "create an index name with suffix when suffix is set" in {
      val kcql = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX=_suffix_{YYYY-MM-dd}")

      val formattedDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
      CreateIndex.getIndexName(kcql) shouldBe s"index_name_suffix_$formattedDateTime"
    }
  }
}

package com.datamountaineer.streamreactor.connect.elastic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.elastic.indexname.CreateIndex
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{Matchers, WordSpec}

class CreateIndexTest extends WordSpec with Matchers {
  "CreateIndex" should {
    "create an index name without suffix when suffix not set" in {
      val config = Config.parse("INSERT INTO index_name SELECT * FROM topicA")
      CreateIndex.getIndexName(config) shouldBe "index_name"
    }

    "create an index name with suffix when suffix is set" in {
      val config = Config.parse("INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX=_suffix_{YYYY-MM-dd}")

      val formattedDateTime = new DateTime(DateTimeZone.UTC).toString("YYYY-MM-dd")
      CreateIndex.getIndexName(config) shouldBe s"index_name_suffix_$formattedDateTime"
    }
  }
}

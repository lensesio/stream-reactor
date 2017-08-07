package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic.indexname.CreateIndex
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{Matchers, WordSpec}

class CreateIndexTest extends WordSpec with Matchers {
  "CreateIndex" should {
    "create an index name without suffix when suffix not set" in {
      val kcql = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA")
      CreateIndex.getIndexName(kcql) shouldBe "index_name"
    }

    "create an index name with suffix when suffix is set" in {
      val kcql = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX=_suffix_{YYYY-MM-dd}")

      val formattedDateTime = new DateTime(DateTimeZone.UTC).toString("YYYY-MM-dd")
      CreateIndex.getIndexName(kcql) shouldBe s"index_name_suffix_$formattedDateTime"
    }
  }
}

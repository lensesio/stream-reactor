package com.datamountaineer.streamreactor.connect.elastic

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.scalatest.{FlatSpec, Matchers}

class TestElasticJsonWriter extends FlatSpec with Matchers {
  "ElasticJsonWriter" should "create an index name without suffix when suffix not set" in {
    ElasticJsonWriter.createIndexName(None)("index_name") shouldBe "index_name"
  }

  it should "create an index name with suffix when suffix is set" in {
    val formattedDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
    ElasticJsonWriter.createIndexName(Some("_suffix_{YYYY-MM-dd}"))("index_name") shouldBe s"index_name_suffix_$formattedDateTime"
  }
}

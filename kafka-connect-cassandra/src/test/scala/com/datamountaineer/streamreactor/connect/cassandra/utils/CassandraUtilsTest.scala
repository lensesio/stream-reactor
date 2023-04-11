/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.cassandra.utils

import java.time.Instant

import com.datamountaineer.streamreactor.connect.cassandra.config.BucketMode
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CassandraUtilsTest extends AnyWordSpec with Matchers {

  "CassandraUtilsTest.getBucketsBetweenDates" should {

    "return correct single bucket from SECOND bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T18:35:24.55Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.SECOND, "yyMMddHHmmss")
      result.get(0) shouldBe "200812183524"
    }

    "return correct single bucket from MINUTE bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T18:35:58.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.MINUTE, "yyMMddHHmm")
      result.get(0) shouldBe "2008121835"
    }

    "return correct single bucket from HOUR bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T18:55:58.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.HOUR, "yyMMddHH")
      result.get(0) shouldBe "20081218"
    }

    "return correct single bucket from DAY bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T23:35:58.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.DAY, "yyMMdd")
      result.get(0) shouldBe "200812"
    }

    "return correct multiple buckets from SECOND bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T18:35:28.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.SECOND, "yyMMddHHmmss")
      result.size() shouldBe 5
      result.get(0) shouldBe "200812183524"
      result.get(1) shouldBe "200812183525"
      result.get(2) shouldBe "200812183526"
      result.get(3) shouldBe "200812183527"
      result.get(4) shouldBe "200812183528"
    }

    "return correct multiple buckets from MINUTE bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T18:39:20.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.MINUTE, "yyMMddHHmm")
      result.size() shouldBe 5
      result.get(0) shouldBe "2008121835"
      result.get(1) shouldBe "2008121836"
      result.get(2) shouldBe "2008121837"
      result.get(3) shouldBe "2008121838"
      result.get(4) shouldBe "2008121839"
    }

    "return correct multiple buckets from HOUR bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-12T22:39:28.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.HOUR, "yyMMddHH")
      result.size() shouldBe 5
      result.get(0) shouldBe "20081218"
      result.get(1) shouldBe "20081219"
      result.get(2) shouldBe "20081220"
      result.get(3) shouldBe "20081221"
      result.get(4) shouldBe "20081222"
    }

    "return correct multiple buckets from DAY bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-08-16T22:39:20.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.DAY, "yyMMdd")
      result.size() shouldBe 5
      result.get(0) shouldBe "200812"
      result.get(1) shouldBe "200813"
      result.get(2) shouldBe "200814"
      result.get(3) shouldBe "200815"
      result.get(4) shouldBe "200816"
    }

    "return correct quantity of buckets buckets from MINUTE bucket" in {
      val previous   = Instant.parse("2020-08-12T18:35:24.00Z");
      val upperBound = Instant.parse("2020-09-12T22:39:28.10Z");

      val result = CassandraUtils.getBucketsBetweenDates(previous, upperBound, BucketMode.MINUTE, "yyMMddHHmm")
      result.size() shouldBe 44885
    }

  }

}

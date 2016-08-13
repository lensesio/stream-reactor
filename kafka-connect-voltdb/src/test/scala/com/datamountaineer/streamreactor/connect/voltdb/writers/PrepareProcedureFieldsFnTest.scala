package com.datamountaineer.streamreactor.connect.voltdb.writers

import org.scalatest.{Matchers, WordSpec}

class PrepareProcedureFieldsFnTest extends WordSpec with Matchers {
  "PrepareProcedureFieldsFn" should {
    "return null for all fields if they are not found" in {
      val actual = PrepareProcedureFieldsFn(Seq("A", "B", "C"), Map("d" -> 1, "e" -> "aa"))
      val expected = Seq(null, null, null)
      actual shouldBe expected
    }

    "return the values in order of the fields" in {
      val map = Map("A" -> 1, "C" -> Seq(1.toByte, 2.toByte), "B" -> "aa")
      val actual = PrepareProcedureFieldsFn(Seq("A", "B", "C"), map)
      val expected = Seq(1, "aa", Seq(1.toByte, 2.toByte))
      actual shouldBe expected
    }
  }
}

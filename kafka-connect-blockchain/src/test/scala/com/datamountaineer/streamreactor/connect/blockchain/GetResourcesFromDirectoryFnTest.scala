package com.datamountaineer.streamreactor.connect.blockchain

import org.scalatest.{Matchers, WordSpec}

class GetResourcesFromDirectoryFnTest extends WordSpec with Matchers {
  "GetResourcesFromDirectoryFn" should {
    "list all the files in a specified resource folder" in {
      val actualFiles = GetResourcesFromDirectoryFn("/testResources")
      actualFiles.nonEmpty shouldBe true
      actualFiles.head.getName shouldBe "sample.txt"
    }
  }
}

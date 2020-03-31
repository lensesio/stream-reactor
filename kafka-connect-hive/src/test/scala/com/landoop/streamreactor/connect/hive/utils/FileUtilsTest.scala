package com.landoop.streamreactor.connect.hive.utils

import java.io.File

import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FileUtilsTest extends AnyFunSuite with Matchers {
  test("raises an exception if the file does not exists") {
    intercept[ConfigException] {
      FileUtils.throwIfNotExists("does_not_exist.file", "k1")
    }
  }

  test("throws an exception when the path is a directory") {
    val file = new File("dir")
    file.mkdir() shouldBe true
    try {
      intercept[ConfigException] {
        FileUtils.throwIfNotExists(file.getAbsolutePath, "k1")
      }
    }
    finally {
      file.delete()
    }
  }

  test("returns when  the file exists") {
    val file = new File("file1.abc")
    file.createNewFile() shouldBe true
    try {
      FileUtils.throwIfNotExists(file.getAbsolutePath, "k1")
    }
    catch {
      case throwable: Throwable =>
        fail("Should not raise an exception")
    }
    finally {
      file.delete()
    }
  }
}

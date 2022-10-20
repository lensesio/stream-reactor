package io.lenses.streamreactor.connect.aws.s3.model.location

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FileUtilsTest extends AnyFunSuite with Matchers {

  test("create parent files") {
    val tmpDir   = Files.createTempDirectory("FileUtilsTest")
    val filePath = tmpDir.resolve("i/can/haz/cheeseburger.txt")
    val file     = filePath.toFile

    file.exists should be(false)

    FileUtils.createFileAndParents(filePath.toFile)

    file.exists should be(true)
    file.isFile should be(true)
  }

}

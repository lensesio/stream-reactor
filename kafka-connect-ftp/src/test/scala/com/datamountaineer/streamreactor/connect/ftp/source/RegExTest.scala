package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.source
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.net.ftp.FTPFile
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class RegExTest extends AnyFunSuite with Matchers with BeforeAndAfter with StrictLogging with MockitoSugar  {
  def mockFile(name: String) = {
    val f = mock[FTPFile]
    when(f.isFile).thenReturn(true)
    when(f.isDirectory).thenReturn(false)
    when(f.getName()).thenReturn(name)
    f
  }
  test("Matches RegEx"){
    FtpSourceConfig.fileFilter -> ".*"

    var f : source.AbsoluteFtpFile = new AbsoluteFtpFile(mockFile("file.txt"),"\\");
    f.name.matches(".*") shouldBe true
    f.name.matches("a") shouldBe false
  }
}

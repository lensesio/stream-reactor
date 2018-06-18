package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.source
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.net.ftp.FTPFile
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class RegExTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging with MockitoSugar  {
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

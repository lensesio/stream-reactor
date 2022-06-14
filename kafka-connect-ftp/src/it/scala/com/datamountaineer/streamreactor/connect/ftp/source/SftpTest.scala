package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.source.EndToEnd.DummyOffsetStorage
import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.withSftpServer
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class SftpTest extends AnyFunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val lineSep      = System.getProperty("line.separator")
  val fileContent1 = (0 to 10000).map(index => s"line_${index}${lineSep}").mkString.getBytes
  val fileContent2 = (0 to 11000).map(index => s"line_${index}${lineSep}").mkString.getBytes

  test(
    "Sftp:Same content mode with SimpleFileConverter : " +
      "after update file with same data, we detect same info so no data must be sent") {

    withSftpServer { server =>
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorTail, "/directory/:sftp_ouptut")
        .updated(FtpSourceConfig.FileMaxAge, "PT952302H53M5.962S")
        .updated(FtpSourceConfig.KeyStyle, "struct")
        .updated(FtpSourceConfig.fileFilter, ".*")

      val cfg = new FtpSourceConfig(configMap.asJava)

      val poller = new FtpSourcePoller(cfg, offsets)

      //push file
      server.putFile("/directory/file1.txt", fileContent1)
      server.putFile("/directory/file1.txt", fileContent1)

      val allReadBytes: Array[Byte] = sftpPollUntilEnd(poller)

      allReadBytes.length shouldBe fileContent1.size
      allReadBytes shouldBe fileContent1

      logger.info(s"===================================================")

      //append same content to file
      server.putFile("/directory/file1.txt", fileContent1)

      val allReadBytes1: Array[Byte] = sftpPollUntilEnd(poller)

      //No event is generated
      allReadBytes1.length shouldBe 0
      ()
    }
  }

  test(
    "Sftp:Update mode with MonitorUpdate and SimpleFileConverter :" +
      " after update of file, all file data must be sent") {
    withSftpServer { server =>
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorUpdate, "/directory/:sftp_ouptut")
        .updated(FtpSourceConfig.FileMaxAge, "PT952302H53M5.962S")
        .updated(FtpSourceConfig.KeyStyle, "struct")
        .updated(FtpSourceConfig.fileFilter, ".*")

      val cfg = new FtpSourceConfig(configMap.asJava)

      val poller = new FtpSourcePoller(cfg, offsets)

      //push file
      server.putFile("/directory/file1.txt", fileContent1)

      val allReadBytes: Array[Byte] = sftpPollUntilEnd(poller)

      allReadBytes.length shouldBe fileContent1.size
      allReadBytes shouldBe fileContent1

      logger.info(s"===================================================")

      //append content to file
      val deltaContent = "extra".getBytes
      server.putFile("/directory/file1.txt", fileContent1 ++ deltaContent)

      val allReadBytes1: Array[Byte] = sftpPollUntilEnd(poller)

      //Only the new delta
      allReadBytes1.length shouldBe (fileContent1.size + deltaContent.size)
      ()
    }
  }

  test(
    "Sftp:Update mode with MonitorTail and SimpleFileConverter :" +
      " after update of file, only new data must be sent") {
    withSftpServer { server =>
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorTail, "/directory/:sftp_ouptut")
        .updated(FtpSourceConfig.FileMaxAge, "PT952302H53M5.962S")
        .updated(FtpSourceConfig.KeyStyle, "struct")
        .updated(FtpSourceConfig.fileFilter, ".*")

      val cfg = new FtpSourceConfig(configMap.asJava)

      val poller: FtpSourcePoller = new FtpSourcePoller(cfg, offsets)

      //push file
      server.putFile("/directory/file1.txt", fileContent1)

      val allReadBytes: Array[Byte] = sftpPollUntilEnd(poller)

      allReadBytes.length shouldBe fileContent1.size
      allReadBytes shouldBe fileContent1

      logger.info(s"===================================================")

      //append content to file
      val deltaContent = "extra".getBytes
      server.putFile("/directory/file1.txt", fileContent1 ++ deltaContent)

      val allReadBytes1: Array[Byte] = sftpPollUntilEnd(poller)

      //Only the new delta
      allReadBytes1.length shouldBe deltaContent.size
      ()
    }
  }

  def sftpPollUntilEnd(poller: FtpSourcePoller): Array[Byte] = {
    var cnt = 0
    logger.info("--------------------poll" + cnt + "-------------------------")
    var slice        = waitForSlice(poller)
    var allReadBytes = new Array[Byte](0)
    while (slice.lengthCompare(1) == 0) {
      slice.head.topic shouldBe "sftp_ouptut"
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      logger.info(s"bytes.size=${bytes.length}")
      cnt += 1
      logger.info(s"--------------------poll$cnt-------------------------")
      slice = waitForSlice(poller)
    }
    allReadBytes
  }

  private def waitForSlice(poller: FtpSourcePoller): Seq[SourceRecord] = {
    var slice: Seq[SourceRecord] = poller.poll()
    eventually {
      Thread.sleep(500)
      slice = poller.poll()
      println(slice)
      slice should not be null
    }
    slice
  }
}

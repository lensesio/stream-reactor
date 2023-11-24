package io.lenses.streamreactor.connect.ftp.source

import io.lenses.streamreactor.connect.ftp.source.EndToEnd.DummyOffsetStorage
import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.withSftpServer
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class SftpBySliceTest extends AnyFunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val lineSep      = System.getProperty("line.separator")
  val fileContent1 = (0 to 10000).map(index => s"line_${index}${lineSep}").mkString.getBytes

  test(
    "Sftp:Same content mode by slices mode with SimpleFileConverter",
  ) {

    withSftpServer { server =>
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorTail, "/directory/:sftp_update_slice")
        .updated(FtpSourceConfig.MonitorSliceSize, "20480")
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
      ()
    }
  }

  def sftpPollUntilEnd(poller: FtpSourcePoller): Array[Byte] = {
    var cnt = 0
    logger.info("--------------------poll" + cnt + "-------------------------")
    var slice        = waitForSlice(poller)
    var allReadBytes = new Array[Byte](0)
    while (slice.lengthCompare(1) == 0) {
      slice.head.topic shouldBe "sftp_update_slice"
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
      Thread.sleep(100)
      slice = poller.poll()
      println(slice)
      slice should not be null
    }
    slice
  }
}

package com.datamountaineer.streamreactor.connect.ftp.source

import com.datamountaineer.streamreactor.connect.ftp.EmbeddedFtpServer
import com.datamountaineer.streamreactor.connect.ftp.source.EndToEnd.Append
import com.datamountaineer.streamreactor.connect.ftp.source.EndToEnd.DummyOffsetStorage
import com.datamountaineer.streamreactor.connect.ftp.source.EndToEnd.FileSystem
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava

class BySlicesTest extends AnyFunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val ftpServer = new EmbeddedFtpServer()

  val lineSep      = System.getProperty("line.separator")
  val fileContent1 = (0 to 10000).map(index => s"line_${index}${lineSep}").mkString.getBytes
  val fileContent2 = (0 to 11000).map(index => s"line_${index}${lineSep}").mkString.getBytes

  val sliceSize = 1024 * 5

  val filePathUpdate = "/update_slice/t0"
  val filePathTail   = "/tail_slice/t0"

  val configUpdateWithSimpleFileConverter = Map(
    FtpSourceConfig.Address          -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User             -> ftpServer.username,
    FtpSourceConfig.Password         -> ftpServer.password,
    FtpSourceConfig.RefreshRate      -> "PT0S",
    FtpSourceConfig.MonitorUpdate    -> "/update_slice/:update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge       -> "P7D",
    FtpSourceConfig.KeyStyle         -> "string",
    FtpSourceConfig.fileFilter       -> ".*",
    FtpSourceConfig.FileConverter    -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter",
  )

  val configTailWithSimpleFileConverter = Map(
    FtpSourceConfig.Address          -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User             -> ftpServer.username,
    FtpSourceConfig.Password         -> ftpServer.password,
    FtpSourceConfig.RefreshRate      -> "PT0S",
    FtpSourceConfig.MonitorTail      -> "/tail_slice/:tail_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge       -> "P7D",
    FtpSourceConfig.KeyStyle         -> "string",
    FtpSourceConfig.fileFilter       -> ".*",
    FtpSourceConfig.FileConverter    -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter",
  )

  val configWithMaxLinesFileConverter = Map(
    FtpSourceConfig.Address          -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User             -> ftpServer.username,
    FtpSourceConfig.Password         -> ftpServer.password,
    FtpSourceConfig.RefreshRate      -> "PT0S",
    FtpSourceConfig.MonitorUpdate    -> "/update_slice/:update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge       -> "P7D",
    FtpSourceConfig.KeyStyle         -> "string",
    FtpSourceConfig.fileFilter       -> ".*",
    FtpSourceConfig.FileConverter    -> "com.datamountaineer.streamreactor.connect.ftp.source.MaxLinesFileConverter",
  )

  val configSftpUpdateWithSimpleFileConverter = Map(
    FtpSourceConfig.Address          -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.Address          -> "localhost",
    FtpSourceConfig.protocol         -> "sftp",
    FtpSourceConfig.User             -> "demo",
    FtpSourceConfig.Password         -> "password",
    FtpSourceConfig.RefreshRate      -> "PT0S",
    FtpSourceConfig.MonitorUpdate    -> "/directory/:sftp_update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge       -> "P7D",
    FtpSourceConfig.KeyStyle         -> "string",
    FtpSourceConfig.fileFilter       -> ".*",
    FtpSourceConfig.FileConverter    -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter",
  )

  logger.info(s"fileContent1.size=${fileContent1.size}")
  logger.info(s"fileContent2.size¤${fileContent2.size}")

  def pollUntilEnd(poller: FtpSourcePoller, expectedTopic: String): Array[Byte] = {
    ftpServer.start()
    var cnt = 0
    logger.info(s"--------------------poll${cnt}-------------------------")
    var slice        = poller.poll()
    var allReadBytes = new Array[Byte](0)
    while (slice.size == 1) {
      slice.head.topic shouldBe expectedTopic
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      logger.info(s"bytes.size=${bytes.size}")
      cnt += 1
      logger.info(s"--------------------polll${cnt}-------------------------")
      slice = poller.poll()
    }
    ftpServer.stop()
    allReadBytes
  }

  test("Update mode by slices mode with SimpleFileConverter : file content is ingested with no loss of data") {
    val fs = new FileSystem(ftpServer.rootDir).clear()

    val cfg =
      new FtpSourceConfig(configUpdateWithSimpleFileConverter.updated(FtpSourceConfig.KeyStyle, "struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller  = new FtpSourcePoller(cfg, offsets)

    //push file
    fs.applyChanges(Seq(filePathUpdate -> Append(fileContent1)))

    var readBytes = pollUntilEnd(poller, "update_slice")
    val asString  = new String(readBytes)
    logger.info("BYTES AS String {}", asString)

    readBytes shouldBe fileContent1
    readBytes.length shouldBe fileContent1.length

    logger.info(s"===================================================")

    //append content to file
    fs.applyChanges(Seq(filePathUpdate -> Append(fileContent2)))
    readBytes = pollUntilEnd(poller, "update_slice")

    val expectedBytes = fileContent1 ++ fileContent2
    readBytes.length shouldBe expectedBytes.length
    readBytes shouldBe expectedBytes
  }

  test("Tail mode by slices mode with SimpleFileConverter : file content is ingested with no loss of data") {
    val fs = new FileSystem(ftpServer.rootDir).clear()

    val cfg = new FtpSourceConfig(configTailWithSimpleFileConverter.updated(FtpSourceConfig.KeyStyle, "struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller  = new FtpSourcePoller(cfg, offsets)

    //push file
    fs.applyChanges(Seq(filePathTail -> Append(fileContent1)))
    var readBytes = pollUntilEnd(poller, "tail_slice")
    readBytes.size shouldBe fileContent1.size
    readBytes shouldBe fileContent1

    logger.info(s"===================================================")
    //append content to file
    fs.applyChanges(Seq(filePathTail -> Append(fileContent2)))
    readBytes = pollUntilEnd(poller, "tail_slice")

    readBytes.size shouldBe fileContent2.size
    readBytes shouldBe fileContent2

  }

  test(
    "Update mode by slices with MaxLinesFileConverter : no loss of data and sent by blocs of lines to the RecordConverter",
  ) {
    ftpServer.start()
    val fs = new FileSystem(ftpServer.rootDir).clear()

    val cfg = new FtpSourceConfig(configWithMaxLinesFileConverter.updated(FtpSourceConfig.KeyStyle, "struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller  = new FtpSourcePoller(cfg, offsets)

    fs.applyChanges(Seq(filePathUpdate -> Append(fileContent1)))

    logger.info(s"--------------------poll-------------------------")
    var slice = poller.poll()

    var allReadBytes = new Array[Byte](0)
    while (slice.size == 1) {
      slice.head.topic shouldBe "update_slice"
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      val lastBytes = util.Arrays.copyOfRange(bytes, bytes.size - lineSep.getBytes.size, bytes.size)

      lastBytes shouldBe lineSep.getBytes

      logger.info(s"bytes.size=${bytes.size}")
      logger.info(s"lastBytes=${lastBytes}")
      logger.info(s"--------------------poll-------------------------")
      slice = poller.poll()
    }
    allReadBytes shouldBe fileContent1
    ftpServer.stop()
  }
}

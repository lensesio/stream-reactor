package com.datamountaineer.streamreactor.connect.ftp.source


import com.datamountaineer.streamreactor.connect.ftp.source.EndToEnd.{Append, DummyOffsetStorage, EmbeddedFtpServer, FileSystem}
import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.withSftpServer
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util
import scala.collection.JavaConverters._


class BySlicesTest extends AnyFunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val ftpServer = new EmbeddedFtpServer(3333)

  val lineSep = System.getProperty("line.separator")
  val fileContent1 = (0 to 10000).map(index => s"line_${index}${lineSep}").mkString.getBytes
  val fileContent2 = (0 to 11000).map(index => s"line_${index}${lineSep}").mkString.getBytes

  val sliceSize = 1024 * 5

  val filePathUpdate = "/update_slice/t0"
  val filePathTail = "/tail_slice/t0"

  val configUpdateWithSimpleFileConverter = Map(FtpSourceConfig.Address -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User -> ftpServer.username,
    FtpSourceConfig.Password -> ftpServer.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorUpdate -> "/update_slice/:update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FileConverter -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter"
  )

  val configTailWithSimpleFileConverter = Map(FtpSourceConfig.Address -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User -> ftpServer.username,
    FtpSourceConfig.Password -> ftpServer.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorTail -> "/tail_slice/:tail_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FileConverter -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter"
  )

  val configWithMaxLinesFileConverter = Map(FtpSourceConfig.Address -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User -> ftpServer.username,
    FtpSourceConfig.Password -> ftpServer.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorUpdate -> "/update_slice/:update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FileConverter -> "com.datamountaineer.streamreactor.connect.ftp.source.MaxLinesFileConverter"
  )

  val configSftpUpdateWithSimpleFileConverter = Map(FtpSourceConfig.Address -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.Address -> "localhost",
    FtpSourceConfig.protocol -> "sftp",
    FtpSourceConfig.User -> "demo",
    FtpSourceConfig.Password -> "password",
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorUpdate -> "/directory/:sftp_update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FileConverter -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter"
  )

  logger.info(s"fileContent1.size=${fileContent1.size}")
  logger.info(s"fileContent2.size¤${fileContent2.size}")

  def pollUntilEnd(poller: FtpSourcePoller, expectedTopic: String): Array[Byte] = {
    var cnt = 0
    logger.info(s"--------------------poll${cnt}-------------------------")
    ftpServer.start()
    var slice = poller.poll()
    ftpServer.stop()
    var allReadBytes = new Array[Byte](0)
    while (slice.size == 1) {
      slice.head.topic shouldBe expectedTopic
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      logger.info(s"bytes.size=${bytes.size}")
      cnt += 1
      logger.info(s"--------------------polll${cnt}-------------------------")
      ftpServer.start()
      slice = poller.poll()
      ftpServer.stop()
    }
    return allReadBytes
  }

  test("Update mode by slices mode with SimpleFileConverter : file content is ingested with no loss of data") {
    val fs = new FileSystem(ftpServer.rootDir).clear

    val cfg = new FtpSourceConfig(configUpdateWithSimpleFileConverter.updated(FtpSourceConfig.KeyStyle, "struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)


    //push file
    fs.applyChanges(Seq(filePathUpdate -> Append(fileContent1)))

    var readBytes = pollUntilEnd(poller, "update_slice")
    readBytes.size shouldBe fileContent1.size
    readBytes.deep shouldBe fileContent1.deep

    logger.info(s"===================================================")

    //append content to file
    fs.applyChanges(Seq(filePathUpdate -> Append(fileContent2)))
    readBytes = pollUntilEnd(poller, "update_slice")
    //ftpServer.stop()

    val expectedBytes = (fileContent1 ++ fileContent2)
    readBytes.size shouldBe expectedBytes.size
    readBytes.deep shouldBe expectedBytes.deep
  }


  test("Tail mode by slices mode with SimpleFileConverter : file content is ingested with no loss of data") {
    val fs = new FileSystem(ftpServer.rootDir).clear

    val cfg = new FtpSourceConfig(configTailWithSimpleFileConverter.updated(FtpSourceConfig.KeyStyle, "struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)

    //push file
    fs.applyChanges(Seq(filePathTail -> Append(fileContent1)))
    var readBytes = pollUntilEnd(poller, "tail_slice")
    readBytes.size shouldBe fileContent1.size
    readBytes.deep shouldBe fileContent1.deep

    logger.info(s"===================================================")
    //append content to file
    fs.applyChanges(Seq(filePathTail -> Append(fileContent2)))
    readBytes = pollUntilEnd(poller, "tail_slice")

    readBytes.size shouldBe fileContent2.size
    readBytes.deep shouldBe fileContent2.deep

  }

  test("Update mode by slices with MaxLinesFileConverter : no loss of data and sent by blocs of lines to the RecordConverter") {
    val fs = new FileSystem(ftpServer.rootDir).clear

    val cfg = new FtpSourceConfig(configWithMaxLinesFileConverter.updated(FtpSourceConfig.KeyStyle, "struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)

    fs.applyChanges(Seq(filePathUpdate -> Append(fileContent1)))

    logger.info(s"--------------------poll-------------------------")
    ftpServer.start()
    var slice = poller.poll()
    ftpServer.stop()

    var allReadBytes = new Array[Byte](0)
    while (slice.size == 1) {
      slice.head.topic shouldBe "update_slice"
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      val lastBytes = util.Arrays.copyOfRange(bytes, bytes.size - lineSep.getBytes.size, bytes.size)

      lastBytes.deep shouldBe lineSep.getBytes.deep

      logger.info(s"bytes.size=${bytes.size}")
      logger.info(s"lastBytes=${lastBytes.deep}")
      logger.info(s"--------------------poll-------------------------")
      ftpServer.start()
      slice = poller.poll()
      ftpServer.stop()
    }
    allReadBytes.deep shouldBe fileContent1.deep
  }

  /**
    * Sftp
    * ------
    */

  test("Sftp:Same content mode by slices mode with SimpleFileConverter : " +
    "after update file with same data, we detect same info so no data must be sent") {

    withSftpServer(server => {
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorTail, "/directory/:sftp_update_slice")
        .updated(FtpSourceConfig.FileMaxAge, "PT952302H53M5.962S")
        .updated(FtpSourceConfig.KeyStyle, "struct")
        .updated(FtpSourceConfig.fileFilter, ".*")

      val cfg = new FtpSourceConfig(configMap.asJava)

      val poller = new FtpSourcePoller(cfg, offsets)

      //push file
      server.putFile("/directory/file1.txt", fileContent1)


      val allReadBytes: Array[Byte] = sftpPollUntilEnd(poller)

      allReadBytes.length shouldBe fileContent1.size
      allReadBytes.deep shouldBe fileContent1.deep

      logger.info(s"===================================================")

      //append same content to file
      server.putFile("/directory/file1.txt", fileContent1)

      val allReadBytes1: Array[Byte] = sftpPollUntilEnd(poller)

      //No event is generated
      allReadBytes1.length shouldBe 0
    })
  }

  test("Sftp:Update mode by slices mode with MonitorUpdate and SimpleFileConverter :" +
    " after update of file, all file data must be sent") {
    withSftpServer(server => {
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorUpdate, "/directory/:sftp_update_slice")
        .updated(FtpSourceConfig.FileMaxAge, "PT952302H53M5.962S")
        .updated(FtpSourceConfig.KeyStyle, "struct")
        .updated(FtpSourceConfig.fileFilter, ".*")

      val cfg = new FtpSourceConfig(configMap.asJava)

      val poller = new FtpSourcePoller(cfg, offsets)

      //push file
      server.putFile("/directory/file1.txt", fileContent1)


      val allReadBytes: Array[Byte] = sftpPollUntilEnd(poller)

      allReadBytes.length shouldBe fileContent1.size
      allReadBytes.deep shouldBe fileContent1.deep

      logger.info(s"===================================================")

      //append content to file
      val deltaContent = "extra".getBytes
      server.putFile("/directory/file1.txt", fileContent1 ++ deltaContent)

      val allReadBytes1: Array[Byte] = sftpPollUntilEnd(poller)

      //Only the new delta
      allReadBytes1.length shouldBe (fileContent1.size + deltaContent.size)
    })
  }

  test("Sftp:Update mode by slices mode with MonitorTail and SimpleFileConverter :" +
    " after update of file, only new data must be sent") {
    withSftpServer(server => {
      server.addUser("demo", "password")

      val offsets = new DummyOffsetStorage
      val configMap = Map()
        .updated(FtpSourceConfig.Address, s"localhost:${server.getPort}")
        .updated(FtpSourceConfig.protocol, "sftp")
        .updated(FtpSourceConfig.User, "demo")
        .updated(FtpSourceConfig.Password, "password")
        .updated(FtpSourceConfig.RefreshRate, "PT1S")
        .updated(FtpSourceConfig.MonitorTail, "/directory/:sftp_update_slice")
        .updated(FtpSourceConfig.FileMaxAge, "PT952302H53M5.962S")
        .updated(FtpSourceConfig.KeyStyle, "struct")
        .updated(FtpSourceConfig.fileFilter, ".*")

      val cfg = new FtpSourceConfig(configMap.asJava)

      val poller: FtpSourcePoller = new FtpSourcePoller(cfg, offsets)

      //push file
      server.putFile("/directory/file1.txt", fileContent1)


      val allReadBytes: Array[Byte] = sftpPollUntilEnd(poller)

      allReadBytes.length shouldBe fileContent1.size
      allReadBytes.deep shouldBe fileContent1.deep

      logger.info(s"===================================================")

      //append content to file
      val deltaContent = "extra".getBytes
      server.putFile("/directory/file1.txt", fileContent1 ++ deltaContent)

      val allReadBytes1: Array[Byte] = sftpPollUntilEnd(poller)

      //Only the new delta
      allReadBytes1.length shouldBe deltaContent.size
    })
  }

  def sftpPollUntilEnd(poller: FtpSourcePoller): Array[Byte] = {
    var cnt = 0
    logger.info("--------------------poll" + cnt + "-------------------------")
    var slice = waitForSlice(poller)
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

  private def waitForSlice(poller: FtpSourcePoller): Stream[SourceRecord] = {
    var slice: Stream[SourceRecord] = poller.poll()
    eventually {
      Thread.sleep(500)
      slice = poller.poll()
      println(slice)
      slice should not be null
    }
    slice
  }
}

package com.datamountaineer.streamreactor.connect.ftp.source

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters._


class UpdateSliceModeTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val server = new EmbeddedFtpServer

  val sliceSize = 4096

  val configWithSimpleFileConverter = Map(FtpSourceConfig.Address -> s"${server.host}:${server.port}",
    FtpSourceConfig.User -> server.username,
    FtpSourceConfig.Password -> server.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorUpdate -> "/update_slice/:update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FileConverter -> "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter"
  )

  val configWithMaxLinesFileConverter = Map(FtpSourceConfig.Address -> s"${server.host}:${server.port}",
    FtpSourceConfig.User -> server.username,
    FtpSourceConfig.Password -> server.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorUpdate -> "/update_slice/:update_slice",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FileConverter -> "com.datamountaineer.streamreactor.connect.ftp.source.MaxLinesFileConverter"
  )


  //val sEmpty = new Array[Byte](0)
  val lineSep = System.getProperty("line.separator")
  val fileContent = (0 to 5000).map(index=>s"line_${index}${lineSep}").mkString.getBytes //(0 to 10000).map(_.toByte).toArray

  val f0 = "/update_slice/t0"


  val changeSets = Seq(
    Seq(f0 -> Append(fileContent))
  )
  /*
  test("Slice mode with SimpleFileConverter : file content is ingested with no loss of data") {
    val fs = new FileSystem(server.rootDir).clear

    val cfg = new FtpSourceConfig(configWithSimpleFileConverter.updated(FtpSourceConfig.KeyStyle,"struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)

    server.start()
    poller.poll() shouldBe empty

    changeSets.foreach(changeSet => {
      fs.applyChanges(changeSet)
    })
    logger.info(s"--------------------poll-------------------------")
    var slice = poller.poll()
    var allReadBytes = new Array[Byte](0)
    while (slice.size == 1 ) {
      slice.head.topic shouldBe "update_slice"
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      logger.info(s"bytes.size=${bytes.size}")
      logger.info(s"--------------------poll-------------------------")
      slice = poller.poll()
    }
    server.stop()
    allReadBytes.size shouldBe fileContent.size
    server.stop()
  }
  */


  test("Slice mode with MaxLinesFileConverter : no loss of data and sent by blocs of lines to the RecordConverter") {
    val fs = new FileSystem(server.rootDir).clear

    val cfg = new FtpSourceConfig(configWithMaxLinesFileConverter.updated(FtpSourceConfig.KeyStyle,"struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)

    server.start()

    changeSets.foreach(changeSet => {
      fs.applyChanges(changeSet)
    })

    logger.info(s"--------------------poll-------------------------")
    var slice = poller.poll()
    var allReadBytes = new Array[Byte](0)
    while (slice.size == 1 ) {
      slice.head.topic shouldBe "update_slice"
      val bytes = slice.head.value().asInstanceOf[Array[Byte]]
      allReadBytes ++= bytes
      val lastBytes = util.Arrays.copyOfRange(bytes, bytes.size - lineSep.getBytes.size, bytes.size)

      lastBytes.deep shouldBe lineSep.getBytes.deep

      logger.info(s"bytes.size=${bytes.size}")
      logger.info(s"lastBytes=${lastBytes.deep}")
      logger.info(s"--------------------poll-------------------------")
      slice = poller.poll()
    }
    server.stop()
    allReadBytes.deep shouldBe fileContent.deep
  }
}

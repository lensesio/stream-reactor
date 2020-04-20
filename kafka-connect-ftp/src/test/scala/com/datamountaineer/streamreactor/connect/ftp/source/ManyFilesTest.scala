package com.datamountaineer.streamreactor.connect.ftp.source

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters._


class ManyFilesTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {
  val ftpServer = new EmbeddedFtpServer(3333)

  val fileCount = 132
  val sliceSize = 1024
  val maxPollRecords = 74

  val lineSep = System.getProperty("line.separator")
  val fileContent = (1 to 12000).map(index => s"line_${"%010d".format(index)}${lineSep}").mkString.getBytes

  val fileName = "the_file_name"
  val filePath = s"/folder/${fileName}"

  val sourceConfig = Map(
    FtpSourceConfig.Address -> s"${ftpServer.host}:${ftpServer.port}",
    FtpSourceConfig.User -> ftpServer.username,
    FtpSourceConfig.Password -> ftpServer.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorTail -> "/folder/:output_topic",
    FtpSourceConfig.MonitorSliceSize -> sliceSize.toString,
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*",
    FtpSourceConfig.FtpMaxPollRecords -> s"${maxPollRecords}",
    FtpSourceConfig.KeyStyle -> "struct"
  )


  test("Read only FtpMaxPollRecords even if using MonitorSliceSize") {
    val fs = new FileSystem(ftpServer.rootDir).clear()
    val cfg = new FtpSourceConfig(sourceConfig.asJava)
    val offsets = new DummyOffsetStorage
    (0 to fileCount).map(index => fs.applyChanges(Seq(s"${filePath}_${index}" -> Append(fileContent))))

    val poller = new FtpSourcePoller(cfg, offsets)
    ftpServer.start()
    val slices = poller.poll()
    (slices.size) shouldBe (maxPollRecords)
    ftpServer.stop()
  }
}

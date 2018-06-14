package com.datamountaineer.streamreactor.connect.ftp.source

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import scala.collection.JavaConverters._


class UpdateSliceModeTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {

  val server = new EmbeddedFtpServer

  val sliceSize = 4096

  val defaultConfig = Map(FtpSourceConfig.Address -> s"${server.host}:${server.port}",
    FtpSourceConfig.User -> server.username,
    FtpSourceConfig.Password -> server.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorUpdateSlice -> "/update_slice/:update_slice",
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*"
  )

  val sEmpty = new Array[Byte](0)
  val fileContent = (0 to 10000).map(_.toByte).toArray

  val f0 = "/update_slice/t0"


  val changeSets = Seq(
    Seq(f0 -> Append(fileContent))
  )

  test("Slice mode : file content is ingested with no loss of data") {
    val fs = new FileSystem(server.rootDir).clear

    val cfg = new FtpSourceConfig(defaultConfig.updated(FtpSourceConfig.KeyStyle,"struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)

    server.start()
    poller.poll() shouldBe empty

    changeSets.foreach(changeSet => {
      val diffs = fs.applyChanges(changeSet)
    })

    val expectedLastSliceSize = fileContent.length % sliceSize
    val expectedNumberOfFullSlices = ((fileContent.length - expectedLastSliceSize) /  sliceSize)

    for( i <- 1 to expectedNumberOfFullSlices){
      logger.info(s"--------------------poll${i}-------------------------")
      val slice = poller.poll()
      slice should have size 1
      slice.head.topic shouldBe "update_slice"
      slice.head.value().asInstanceOf[Array[Byte]].size shouldBe sliceSize
    }
    //check last slice
    logger.info(s"--------------------poll for last slice-------------------------")
    val lastSlice = poller.poll()
    lastSlice should have size 1
    lastSlice.head.topic shouldBe "update_slice"
    lastSlice.head.value().asInstanceOf[Array[Byte]].size shouldBe expectedLastSliceSize

    logger.info(s"--------------------poll next slice-------------------------")
    poller.poll() shouldBe empty
  }
}

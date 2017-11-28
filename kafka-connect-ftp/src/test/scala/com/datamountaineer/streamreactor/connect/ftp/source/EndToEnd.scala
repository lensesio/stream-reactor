/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.ftp.source

import java.nio.file.Path
import java.util

import better.files._
import com.datamountaineer.streamreactor.connect.ftp.source.KeyStyle.KeyStyle
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.{FtpServer, FtpServerFactory}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters._

// TODO: to be done for more advanced tests
class DummyOffsetStorage extends OffsetStorageReader {
  override def offset[T](map: util.Map[String, T]): util.Map[String, AnyRef] = {
    null //Map[String,AnyRef]().asJava
  }

  override def offsets[T](collection: util.Collection[util.Map[String, T]]): util.Map[util.Map[String, T], util.Map[String, AnyRef]] = {
    null //Map[util.Map[String, T], util.Map[String, AnyRef]]().asJava
  }
}

class EmbeddedFtpServer extends StrictLogging {
  val username = "my-User_name7"
  val password = "=541%2@$;;'`"
  val host = "localhost"
  val port = 3332 // TODO: find free port
  val rootDir = File.newTemporaryDirectory().path

  var server: FtpServer = null

  def start() = {
    val userManagerFactory = new PropertiesUserManagerFactory()
    val userManager = userManagerFactory.createUserManager()
    val user = new BaseUser()
    user.setName(username)
    user.setPassword(password)
    user.setHomeDirectory(rootDir.toString)
    userManager.save(user)
    val serverFactory = new FtpServerFactory()
    val listenerFactory = new ListenerFactory()
    listenerFactory.setPort(port)
    listenerFactory.setServerAddress(host)
    serverFactory.setUserManager(userManager)
    serverFactory.addListener("default", listenerFactory.createListener())
    server = serverFactory.createServer()
    server.start()
  }

  def stop() = server.stop()
}

case class FileDiff(body: Array[Byte], offset: Long =0) {
  def appended(apBody: Array[Byte]) = FileDiff(apBody, body.length.toLong)
  def updated(upBody: Array[Byte]) = FileDiff(upBody, 0)
}

trait FileChange
case class Update(body:Array[Byte]) extends FileChange
case class Append(body:Array[Byte]) extends FileChange

class FileSystem(rootDir:Path) {
  def clear() = {
    require(File(rootDir).isDirectory)
    File(rootDir).clear()
    this
  }

  def file(path: String): File = {
    val realPath = rootDir.resolve("." + path)
    File(realPath.getParent).createIfNotExists(asDirectory = true)
    File(realPath)
  }

  def applyChanges(chgs: Seq[(String, FileChange)]): Seq[(String, FileDiff)] = {
    chgs.flatMap { case (fn, chg) => chg match {
      case Update(body) => {
        file(fn).writeBytes(body.iterator)
        Some(fn -> FileDiff(body, 0))
      }
      case Append(body) if body.length > 0 => {
        val oldSize = if (file(fn).exists) file(fn).size else 0
        file(fn).appendBytes(body.iterator)
        Some(fn -> FileDiff(body, oldSize))
      }
      case _ => None
    }
    }
  }
}

// spins up an embedded ftp server, updates files, uses FtpSourcePoller to obtain SourceRecords which are verified
class EndToEndTests extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {
  val sEmpty = new Array[Byte](0)
  val s0 = (0 to 255).map(_.toByte).toArray
  val s1 = "Hebban olla vogala nestas hagunnan hinase hic enda thu wat unbidan we nu\r\n\t\u0000:)".getBytes
  val s2 = "<mandatory quote to show off erudition here>".getBytes
  val s3 = "!".getBytes
  val t0 = "/tails/t0"
  val t1 = "/tails/t1"
  val u0 = "/updates/u0"
  val u1 = "/updates/u1"

  val changeSets = Seq(
    Seq(
      t0 -> Append(s0),
      t1 -> Append(sEmpty),
      u0 -> Update(s1),
      u1 -> Update(sEmpty)),
    Seq(
      t0 -> Append(s1),
      t1 -> Append(s3),
      u0 -> Update(sEmpty),
      u1 -> Update(s2)),
    Seq(
      t0 -> Append(s3),
      u1 -> Update(s1)),
    Seq(
      t1 -> Append(s1),
      u0 -> Update(s2)),
    Seq(
      t0 -> Append(s0),
      t1 -> Append(s2),
      u0 -> Update(s3),
      u1 -> Update(s2))
  )

  val server = new EmbeddedFtpServer

  val defaultConfig = Map(FtpSourceConfig.Address -> s"${server.host}:${server.port}",
    FtpSourceConfig.User -> server.username,
    FtpSourceConfig.Password -> server.password,
    FtpSourceConfig.RefreshRate -> "PT0S",
    FtpSourceConfig.MonitorTail -> "/tails/:tails",
    FtpSourceConfig.MonitorUpdate -> "/updates/:updates",
    FtpSourceConfig.FileMaxAge -> "P7D",
    FtpSourceConfig.KeyStyle -> "string",
    FtpSourceConfig.fileFilter -> ".*"

  )

  def validateSourceRecords(recs:Seq[SourceRecord], diffs:Seq[(String,String,FileDiff)], keyStyle: KeyStyle = KeyStyle.String) = {
    diffs.length shouldEqual recs.length
    diffs.foreach { case (topic, fileName, diff) =>
    val exist = keyStyle match {
      case KeyStyle.String => recs.exists(rec => rec.topic() == topic && rec.key.asInstanceOf[String] == fileName && util.Arrays.equals(rec.value.asInstanceOf[Array[Byte]], diff.body))
      case KeyStyle.Struct => recs.exists(rec =>
        {
          val keyStruct = rec.key.asInstanceOf[Struct]
          val name = keyStruct.get("name").asInstanceOf[String]
          val offset = keyStruct.get("offset").asInstanceOf[Long]
          rec.topic() == topic && fileName == name && offset == diff.offset && util.Arrays.equals(rec.value.asInstanceOf[Array[Byte]], diff.body)
        })
    }
      exist shouldBe true
      logger.info(s"got a source record that corresponds with the changes on ${fileName}")
    }
  }

  val fileToTopic = Map(t0 -> "tails", t1 -> "tails", u0 -> "updates", u1 -> "updates")

  test("Happy flow: file updates are properly reflected by corresponding SourceRecords with structured keys") {


    val fs = new FileSystem(server.rootDir).clear
    server.start()

    val cfg = new FtpSourceConfig(defaultConfig.updated(FtpSourceConfig.KeyStyle,"struct").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)
    poller.poll() shouldBe empty

    changeSets.foreach(changeSet => {
      val diffs = fs.applyChanges(changeSet)
      validateSourceRecords(poller.poll(), diffs.map { case (f, d) => (fileToTopic(f), f, d) },keyStyle = KeyStyle.Struct)
    })

    server.stop()
  }


  test("Happy flow: file updates are properly reflected by corresponding SourceRecords with stringed keys") {
    val fs = new FileSystem(server.rootDir).clear
    server.start()

    val cfg = new FtpSourceConfig(defaultConfig.updated(FtpSourceConfig.KeyStyle,"string").asJava)

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)
    poller.poll() shouldBe empty

    changeSets.foreach(changeSet => {
      val diffs = fs.applyChanges(changeSet)
      validateSourceRecords(poller.poll(), diffs.map { case (f, d) => (fileToTopic(f), f, d) },keyStyle = KeyStyle.String)
    })

    server.stop()
  }

  test("Streaming flow: files are only fetched when the records are polled") {
    logger.info("Start test")
    val fs = new FileSystem(server.rootDir).clear
    server.start()

    val cfg = new FtpSourceConfig(
      defaultConfig
        .updated(FtpSourceConfig.KeyStyle,"string")
        .updated(FtpSourceConfig.FtpMaxPollRecords, "1").asJava
    )

    val offsets = new DummyOffsetStorage
    val poller = new FtpSourcePoller(cfg, offsets)
    poller.poll() shouldBe empty

    fs.applyChanges(changeSets.head)
    poller.poll().size shouldBe 1

    // clear the ftp directory and the poll will return an empty record,
    // if not it succeeds then the file was pulled before it was needed
    fs.clear()
    poller.poll() // This will return the single cached record
    poller.poll().size shouldBe 0 // Empty because the files have been removed.

    server.stop()
  }
}

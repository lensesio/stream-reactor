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

import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.time.{Duration, Instant}
import java.util

import com.datamountaineer.streamreactor.connect.ftp.source.FtpProtocol.FtpProtocol
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPReply, FTPSClient}
import org.apache.commons.net.{ProtocolCommandEvent, ProtocolCommandListener}

import scala.util.{Failure, Success, Try}

// a full downloaded file
case class FetchedFile(meta:FileMetaData, body: Array[Byte])

// tells to monitor which directory and how files are dealt with, might be better a trait and is TODO
case class MonitoredPath(path:String, tail:Boolean) {
  val p = Paths.get(if (path.endsWith("/")) path + "*" else path)
}

// a potential partial file
case class FileBody(bytes:Array[Byte], offset:Long)
object EmptyFileBody extends FileBody(Array[Byte](), 0)

// instructs the FtpMonitor how to do its things
case class FtpMonitorSettings(host:String, port:Option[Int], user:String, pass:String, maxAge: Option[Duration],
                              directories: Seq[MonitoredPath], timeoutMs:Int, protocol: FtpProtocol, filter:String)

class FtpMonitor(settings:FtpMonitorSettings, fileConverter: FileConverter) extends StrictLogging {
  val MaxAge = settings.maxAge.getOrElse(Duration.ofDays(Long.MaxValue))

  val ftp = settings.protocol match {
    case FtpProtocol.FTP => new FTPClient()
    case FtpProtocol.SFTP => new FTPSClient()
  }

  def requiresFetch(file: AbsoluteFtpFile, metadata: Option[FileMetaData]): Boolean = metadata match {
    case None => logger.debug(s"${file.name} hasn't been seen before"); true
    case Some(known) if known.attribs.size != file.ftpFile.getSize => {
      logger.debug(s"${file.name} size changed ${known.attribs.size} => ${file.size}")
      true
    }
    case Some(known) if known.attribs.timestamp != file.timestamp => {
      logger.debug(s"${file.name} is newer ${known.attribs.timestamp} => ${file.timestamp}")
      true
    }
    case _ => false
  }

  // Retrieves the FtpAbsoluteFile and returns a new or updated KnownFile
  def fetch(file: AbsoluteFtpFile, prevFetch: Option[FileMetaData]): Option[(Option[FileMetaData], FetchedFile)] = {
    logger.info(s"fetching ${file.path}")
    val baos = new ByteArrayOutputStream()

    if (ftp.retrieveFile(file.path, baos)) {
      val bytes = baos.toByteArray
      baos.close()
      val hash = DigestUtils.sha256Hex(bytes)
      val attributes = new FileAttributes(file.path, file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant)
      val meta = prevFetch match {
        case None => FileMetaData(attributes, hash, Instant.now, Instant.now, Instant.now)
        case Some(old) => FileMetaData(attributes, hash, old.firstFetched, old.lastModified, Instant.now)
      }
      Option(prevFetch, FetchedFile(meta, bytes))
    } else {
      logger.warn(s"failed to fetch ${file.path}: ${ftp.getReplyString}")
      None
    }
  }

  // translates a MonitoredDirectory and previously known FileMetaData into a FileMetaData and FileBody
  def handleFetchedFile(tail: Boolean, prevFetch: Option[FileMetaData], current: FetchedFile): (FileMetaData, FileBody) =
    prevFetch match {
      case Some(previously) if previously.attribs.size != current.meta.attribs.size || previously.hash != current.meta.hash =>
        // file changed in size and/or hash
        logger.info(s"fetched ${current.meta.attribs.path}, it was known before and it changed")
        if (tail) {
          if (current.meta.attribs.size > previously.attribs.size) {
            val hashPrevBlock = DigestUtils.sha256Hex(util.Arrays.copyOfRange(current.body, 0, previously.attribs.size.toInt))
            if (previously.hash == hashPrevBlock) {
              logger.info(s"tail ${current.meta.attribs.path} [${previously.attribs.size.toInt}, ${current.meta.attribs.size.toInt})")
              val tail = util.Arrays.copyOfRange(current.body, previously.attribs.size.toInt, current.meta.attribs.size.toInt)
              (current.meta.inspectedNow().modifiedNow(), FileBody(tail, previously.attribs.size))
            } else {
              logger.warn(s"the tail of ${current.meta.attribs.path} is to be followed, but previously seen content changed. we'll provide the entire file.")
              (current.meta.inspectedNow().modifiedNow(), FileBody(current.body, 0))
            }
          } else {
            // the file shrunk or didn't grow
            logger.warn(s"the tail of ${current.meta.attribs.path} is to be followed, but it shrunk")
            (current.meta.inspectedNow().modifiedNow(), EmptyFileBody)
          }
        } else {
          // !tail: we're not tailing but dumping the entire file on change
          logger.info(s"dump entire ${current.meta.attribs.path}")
          (current.meta.inspectedNow().modifiedNow(), FileBody(current.body, 0))
        }
      case Some(_) =>
        // file didn't change
        logger.info(s"fetched ${current.meta.attribs.path}, it was known before and it didn't change")
        (current.meta.inspectedNow(), EmptyFileBody)
      case None =>
        // file is new
        logger.info(s"fetched ${current.meta.attribs.path}, wasn't known before")
        logger.info(s"dump entire ${current.meta.attribs.path}")
        (current.meta.inspectedNow().modifiedNow(), FileBody(current.body, 0))
    }


  // fetches files from a monitored directory when needed
  def fetchFromMonitoredPlaces(w:MonitoredPath): Stream[(FileMetaData, FileBody)] = {

    val files = FtpFileLister(ftp).listFiles(w.p.toString).filter(f => !MaxAge.minus(f.age).isNegative && f.name.matches(settings.filter) )

    logger.info(s"Found ${files.length} items in ${w.p}")

    files.toStream
      // Get the metadata from the offset store
      .map(file => (file , fileConverter.getFileOffset(file.path)))
      // Filter out the files that do not need to be fetched
      .filter{ case (file, offset) => requiresFetch(file, offset) }
      // Fetch the latest file contents
      .flatMap{ case (file, offset) => fetch(file, offset) }
      // Extract the file changes depending on the tracking mode
      .map{ case (prevFile, currentFile) => handleFetchedFile(w.tail, prevFile, currentFile) }
  }

  def connectFtp(): Try[FTPClient] = {
    ftp.disconnect() // TODO: maybe we should keep the connection open. However, the underlying ftp client is a major PITA and this is way easier.
    if (!ftp.isConnected) {
      ftp.setConnectTimeout(settings.timeoutMs)
      ftp.setDefaultTimeout(settings.timeoutMs)
      ftp.setDataTimeout(settings.timeoutMs)
      ftp.setRemoteVerificationEnabled(false)
      ftp.addProtocolCommandListener(new ProtocolCommandListener {
        override def protocolCommandSent(e: ProtocolCommandEvent): Unit = logger.trace(s">> ${e.getCommand} ${e.getMessage} ${e.getReplyCode} ${e.isCommand} ${e.isReply}")

        override def protocolReplyReceived(e: ProtocolCommandEvent): Unit = logger.trace(s"<< ${e.getCommand} ${e.getMessage} ${e.getReplyCode} ${e.isCommand} ${e.isReply}")
      }
      )

      logger.info(s"connect ${settings.host}:${settings.port}")
      settings.port match {
        case Some(explicitPort) => ftp.connect(settings.host, explicitPort)
        case None => ftp.connect(settings.host)
      }
      if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
        ftp.disconnect()
        return Failure(new Exception(s"cannot connect to ftp: ${ftp.getReplyCode}: ${ftp.getReplyString}"))
      }
      ftp.login(settings.user, settings.pass)
      if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
        ftp.disconnect()
        return Failure(new Exception(s"cannot login to ftp: ${ftp.getReplyCode}: ${ftp.getReplyString}"))
      }
      if (!ftp.isConnected) {
        return Failure(new Exception("cannot connect to ftp because of some unreported error"))
      }
      logger.info("successfully connected to the ftp server and logged in")
      ftp.enterLocalPassiveMode()
      logger.info("passive we are")
      ftp.setFileType(FTP.BINARY_FILE_TYPE)
      ftp.setControlKeepAliveTimeout(15) //send NOOP every [seconds]
    }
    Success(ftp)
  }

  def poll(): Try[Stream[(FileMetaData, FileBody, MonitoredPath)]] = connectFtp() match {
      case Success(_) =>
        Try(settings.directories.toStream.flatMap(dir =>
          fetchFromMonitoredPlaces(dir).map { case (meta, body) => (meta, body, dir) }))
      case Failure(err) => logger.warn(s"cannot connect to ftp: ${err.toString}")
        Failure(err)
    }
}
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

package com.datamountaineer.streamreactor.connect.ftp

import java.io.ByteArrayOutputStream
import java.nio.file.{FileSystems, Path, Paths}
import java.time.{Duration, Instant}
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPFile, FTPReply}
import org.apache.commons.net.{ProtocolCommandEvent, ProtocolCommandListener}

import scala.util.{Failure, Success, Try}

// what the ftp can tell us without actually fetching the file
case class FileAttributes(path: String, size: Long, timestamp: Instant) {
  override def toString = s"(path: $path, size: $size, timestamp: $timestamp)"
}

// used to administer the files
// this is persistent data, stored into the connect offsets
case class FileMetaData(attribs:FileAttributes, hash:String, firstFetched:Instant, lastModified:Instant, lastInspected:Instant) {
  def modifiedNow() = FileMetaData(attribs, hash, firstFetched, Instant.now, lastInspected)
  def inspectedNow() = FileMetaData(attribs, hash, firstFetched, lastModified, Instant.now)
  override def toString = s"(remoteInfo: $attribs, hash: $hash, firstFetched: $firstFetched, lastModified: $lastModified, lastInspected: $lastInspected"
}

// a full downloaded file
case class FetchedFile(meta:FileMetaData, body: Array[Byte])

// org.apache.commons.net.ftp.FTPFile only contains the relative path
case class AbsoluteFtpFile(ftpFile:FTPFile, parentDir:String) {
  def path: String = Paths.get(parentDir, ftpFile.getName).toString
  def age: Duration = Duration.between(ftpFile.getTimestamp.toInstant,Instant.now)
}

// tells to monitor which directory and how files are dealt with, might be better a trait and is TODO
case class MonitoredPath(path:String, tail:Boolean) {
  val p: Path = Paths.get(if (path.endsWith("/")) path + "*" else path)

  val pattern: String = p.getFileName.toString  // glob
  val baseDirectory:String = p.getParent.toString match {
    case pp if pp.endsWith("/") => pp
    case pp => pp + "/"
  }

  def isFileRelevant(path:String):Boolean = {
    val g = s"glob:$baseDirectory$pattern"
    FileSystems.getDefault.getPathMatcher(g).matches(Paths.get(path))
  }

  def isFileRelevant(f:AbsoluteFtpFile):Boolean = isFileRelevant(f.path)
}

// a potential partial file
case class FileBody(bytes:Array[Byte], offset:Long)

// instructs the FtpMonitor how to do its things
case class FtpMonitorSettings(host:String, port:Option[Int], user:String, pass:String, maxAge: Option[Duration],directories: Seq[MonitoredPath], timeoutMs:Int)

// the store where FileMetaData is kept and can be retrieved from
trait FileMetaDataStore {
  def get(path:String) : Option[FileMetaData]
  def set(path:String, fileMetaData: FileMetaData)
}

class FtpMonitor(settings:FtpMonitorSettings, knownFiles: FileMetaDataStore) extends StrictLogging {
  val MaxAge: Duration = settings.maxAge.getOrElse(Duration.ofDays(Long.MaxValue))

  val ftp = new FTPClient()

  def requiresFetch(file: AbsoluteFtpFile, knownFile: Option[FileMetaData]):Boolean = knownFile match {
    // file previously unknown? fetch
    case None => !MaxAge.minus(file.age).isNegative
    case Some(known) if known.attribs.size != file.ftpFile.getSize => true
    case Some(known) if known.attribs.timestamp != file.ftpFile.getTimestamp.toInstant => true
    case _ => false
  }

  // Retrieves the FtpAbsoluteFile and returns a new or updated KnownFile
  def fetch(file: AbsoluteFtpFile, knownFile: Option[FileMetaData]): Try[FetchedFile] = {
    logger.info(s"fetch ${file.path}")
    val baos = new ByteArrayOutputStream()
    if (ftp.retrieveFile(file.path, baos)) {
      val bytes = baos.toByteArray
      baos.close()
      val hash = DigestUtils.sha256Hex(bytes)
      Success(FetchedFile(knownFile match {
        case None => FileMetaData(FileAttributes(file.path, file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant), hash, Instant.now, Instant.now, Instant.now)
        case Some(old) => FileMetaData(FileAttributes(file.path, file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant), hash, old.firstFetched, old.lastModified, Instant.now)
      }, bytes))
    } else {
      Failure(new Exception("ftp says no: " + ftp.getReplyString))
    }
  }

  // translates a MonitoredDirectory and previously known FileMetaData into a FileMetaData and FileBody
  def handleFetchedFile(w:MonitoredPath, optPreviously: Option[FileMetaData], current:FetchedFile): (FileMetaData, Option[FileBody]) =
    optPreviously match {
      case Some(previously) if previously.attribs.size != current.meta.attribs.size || previously.hash != current.meta.hash =>
        // file changed in size and/or hash
        logger.info(s"fetched ${current.meta.attribs.path}, it was known before and it changed")
        if (w.tail) {
          if (current.meta.attribs.size > previously.attribs.size) {
            val hashPrevBlock = DigestUtils.sha256Hex(util.Arrays.copyOfRange(current.body, 0, previously.attribs.size.toInt))
            if (previously.hash == hashPrevBlock) {
              logger.info(s"tail ${current.meta.attribs.path} [${previously.attribs.size.toInt}, ${current.meta.attribs.size.toInt})")
              val tail = util.Arrays.copyOfRange(current.body, previously.attribs.size.toInt, current.meta.attribs.size.toInt)
              (current.meta.inspectedNow().modifiedNow(), Some(FileBody(tail,previously.attribs.size)))
            } else {
              logger.warn(s"the tail of ${current.meta.attribs.path} is to be followed, but previously seen content changed. we'll provide the entire file.")
              (current.meta.inspectedNow().modifiedNow(), Some(FileBody(current.body,0)))
            }
          } else {
            // the file shrunk or didn't grow
            logger.warn(s"the tail of ${current.meta.attribs.path} is to be followed, but it shrunk")
            (current.meta.inspectedNow().modifiedNow(), None)
          }
        } else { // !w.tail: we're not tailing but dumping the entire file on change
          logger.info(s"dump entire ${current.meta.attribs.path}")
          (current.meta.inspectedNow().modifiedNow(), Some(FileBody(current.body,0)))
        }
      case Some(_) =>
        // file didn't change
        logger.info(s"fetched ${current.meta.attribs.path}, it was known before and it didn't change")
        (current.meta.inspectedNow(), None)
      case None =>
        // file is new
        logger.info(s"fetched ${current.meta.attribs.path}, wasn't known before")
        logger.info(s"dump entire ${current.meta.attribs.path}")
        (current.meta.inspectedNow().modifiedNow(), Some(FileBody(current.body,0)))
    }

  def debugLogFiles(files:Seq[FTPFile], w:MonitoredPath): Unit = files.foreach(f =>
      {
        logger.debug(s"$f")
        logger.debug(s"${f.getName} is file: ${f.isFile}")
        if (f.isFile) {
          val abs = AbsoluteFtpFile(f, w.baseDirectory)
          logger.debug(s"${f.getName} is relevant according to search pattern: ${w.isFileRelevant(abs)}")
          logger.debug(s"${f.getName} age is ${abs.age}; MaxAge is $MaxAge")
        }
      }
    )


  // fetches files from a monitored directory when needed
  def fetchFromMonitoredPlaces(w:MonitoredPath): Seq[(FileMetaData, Option[FileBody])] = {
    val files = ftp.listFiles(w.baseDirectory).toSeq
    val toBeFetched = files
      .filter(_.isFile)
      .map(AbsoluteFtpFile(_, w.baseDirectory))
      .filter(w.isFileRelevant)
      .filter(f => !MaxAge.minus(f.age).isNegative) // TODO quick HACK: potentially avoid requiresFetch to avoid slow ConnectFileMetaDataStore
      .filter{ f => requiresFetch(f, knownFiles.get(f.path)) }

    debugLogFiles(files, w)

    logger.info(s"we'll be fetching ${toBeFetched.length} items from ${w.baseDirectory}")
    toBeFetched.foreach(f=>
      {
        val kf = knownFiles.get(f.path)
        logger.info(s"we'll be fetching ${f.path} ${f.ftpFile.getSize} ${f.ftpFile.getTimestamp.toInstant} (age: ${f.age})")
        logger.info(s"what we knew from our store of ${f.path}: " + (kf match {
          case Some(file) => s"${file.attribs.size} ${file.attribs.timestamp}"
          case None => "not known before"
        }))
      })

    val previouslyKnown = toBeFetched.map(f => knownFiles.get(f.path))

    val fetchResults = toBeFetched zip previouslyKnown map { case (f, k) => fetch(f, k) }

    toBeFetched zip previouslyKnown zip fetchResults map { case((a,b),c) => (a,b,c)} flatMap {
      case (_, optPrevKnown, Success(currentFile)) => Some(handleFetchedFile(w, optPrevKnown, currentFile))
      case (ftpFile, _, Failure(err)) =>
        logger.warn(s"failed to fetch ${ftpFile.path}: ${err.toString}")
        None
    }
  }

  def connectFtp(): Try[FTPClient] = {
    ftp.disconnect() // TODO: maybe we should keep the connection open. However, the underlying ftp client is a major PITA and this is way easier.
    if (!ftp.isConnected) {
      ftp.setConnectTimeout(settings.timeoutMs)
      ftp.setDefaultTimeout(settings.timeoutMs)
      ftp.setDataTimeout(settings.timeoutMs)
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

  def poll(): Try[Seq[(FileMetaData, FileBody, MonitoredPath)]] = Try(connectFtp() match {
      case Success(_) =>
        val v = settings.directories.flatMap(w => {
          val results: Seq[(FileMetaData, Option[FileBody])] = fetchFromMonitoredPlaces(w)
          results.flatMap {
            case (meta, Some(body)) =>
              logger.info(s"${meta.attribs.path} got @ offset ${body.offset}")
              Some((meta,body, w))
            case (meta, None) =>
              logger.info(s"${meta.attribs.path} got no bytes")
              None
          }
        })
        Success(v)
      case Failure(err) => logger.warn(s"cannot connect to ftp: ${err.toString}")
        Failure(err)
    }).flatten
}

/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.ftp.source

import io.lenses.streamreactor.connect.ftp.source.FtpProtocol.FtpProtocol
import io.lenses.streamreactor.connect.ftp.source.MonitorMode.MonitorMode
import io.lenses.streamreactor.connect.ftp.source.OpTimer.profile
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPReply
import org.apache.commons.net.ftp.FTPSClient
import org.apache.commons.net.ProtocolCommandEvent
import org.apache.commons.net.ProtocolCommandListener

import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

// a full downloaded file
case class FetchedFile(meta: FileMetaData, body: Array[Byte])

// tells to monitor which directory and how files are dealt with, might be better a trait and is TODO
case class MonitoredPath(path: String, mode: MonitorMode) {
  val p = Paths.get(if (path.endsWith("/")) path + "*" else path)
}

// a potential partial file
case class FileBody(bytes: Array[Byte], offset: Long)
object EmptyFileBody extends FileBody(Array[Byte](), 0)

// instructs the FtpMonitor how to do its things
case class FtpMonitorSettings(
  host:        String,
  port:        Option[Int],
  user:        String,
  pass:        String,
  maxAge:      Option[Duration],
  directories: Seq[MonitoredPath],
  timeoutMs:   Int,
  protocol:    FtpProtocol,
  filter:      String,
  sliceSize:   Int,
) {
  val isBySlices = sliceSize > 0
}

class FtpMonitor(settings: FtpMonitorSettings, fileConverter: FileConverter) extends StrictLogging {
  val MaxAge = settings.maxAge.getOrElse(Duration.ofDays(Long.MaxValue))

  val ftp: FTPClient = settings.protocol match {
    case FtpProtocol.FTP  => new FTPClient()
    case FtpProtocol.FTPS => new FTPSClient()
    case FtpProtocol.SFTP => new SFTPClient()

  }

  val sliceSize = settings.sliceSize

  val isBySlices = settings.isBySlices

  def requiresFetch(file: AbsoluteFtpFile, metadata: Option[FileMetaData]): Boolean = metadata match {
    case None => logger.debug(s"${file.name()} hasn't been seen before"); true
    case Some(known) if known.attribs.size != file.ftpFile.getSize => {
      logger.debug(s"${file.name()} size changed ${known.attribs.size} => ${file.size()}")
      true
    }
    case Some(known) if known.attribs.timestamp != file.timestamp() => {
      logger.debug(s"${file.name()} is newer ${known.attribs.timestamp} => ${file.timestamp()}")
      true
    }
    case _ => false
  }

  def bySlicesRequiresFetch(file: AbsoluteFtpFile, metadata: Option[FileMetaData]): Boolean =
    metadata match {
      case None =>
        logger.info(s"${file.name()} hasn't been seen before")
        true
      case Some(previous) if previous.attribs.size != file.ftpFile.getSize => {
        logger.info(s"${file.name()} size changed ${previous.attribs.size} => ${file.size()}")
        true
      }
      case Some(previous) if previous.attribs.timestamp != file.timestamp() => {
        logger.info(s"${file.name()} is newer ${previous.attribs.timestamp} => ${file.timestamp()}")
        true
      }
      case Some(previous) if previous.offset != file.ftpFile.getSize => {
        logger.info(s"${file.name()} was not completely read & committed ${previous.offset} => ${file.ftpFile.getSize}")
        true
      }
      case _ =>
        logger.info(s" no fetch required")
        false
    }

  // Retrieves the FtpAbsoluteFile and returns a new or updated KnownFile
  def fetch(file: AbsoluteFtpFile, prevFetch: Option[FileMetaData]): Option[(Option[FileMetaData], FetchedFile)] = {
    logger.info(s"fetching ${file.path()}")

    val baosOrError = Using(new ByteArrayOutputStream()) {
      baos =>
        ftp.retrieveFile(file.path(), baos)
        baos.toByteArray
    }

    baosOrError match {
      case Failure(exception) =>
        logger.warn(s"failed to fetch ${file.path()}: ${ftp.getReplyString}", exception)
        None
      case Success(bytes) =>
        val hash       = DigestUtils.sha256Hex(bytes)
        val attributes = FileAttributes(file.path(), file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant)
        val meta = prevFetch match {
          case None      => FileMetaData(attributes, hash, Instant.now, Instant.now, Instant.now)
          case Some(old) => FileMetaData(attributes, hash, old.firstFetched, old.lastModified, Instant.now)
        }
        Option((prevFetch, FetchedFile(meta, bytes)))
    }
  }

  def bySlicesGetOffsetToReadFrom(mode: MonitorMode, file: AbsoluteFtpFile, prevFetch: Option[FileMetaData]): Long =
    (prevFetch, mode) match {
      case (None, _) =>
        logger.info(s"First time file is read")
        0
      case (Some(previous), _) if previous.offset == -1 =>
        logger.info(s"First time file is read")
        0
      case (Some(previous), MonitorMode.Update)
          if previous.attribs.timestamp != file.timestamp() || previous.attribs.size != file.size() =>
        logger.info(s"We are in Update mode && File was updated, restart reading from offset 0")
        0
      case (Some(previous), _) =>
        logger.info(s"Continue reading from offset ${previous.offset}")
        previous.offset
    }

  def bySlicesFetch(
    mode:      MonitorMode,
    file:      AbsoluteFtpFile,
    prevFetch: Option[FileMetaData],
  ): Option[(Option[FileMetaData], FetchedFile)] = {

    val offsetToReadFrom = bySlicesGetOffsetToReadFrom(mode, file, prevFetch)

    val errOrBytes = Using(new ByteArrayOutputStream) {
      outputStream =>
        connectFtp()

        ftp.setRestartOffset(offsetToReadFrom)
        Using(ftp.retrieveFileStream(file.path())) {
          inputStream =>
            IOUtils.copyLarge(inputStream, outputStream, 0, sliceSize.toLong)
        } match {
          case Failure(exception) => throw exception
          case Success(_)         => Option.when(outputStream.size() > 0)(outputStream.toByteArray)
        }
    }

    val attributes = profile("createAttributes",
                             FileAttributes(file.path(), file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant),
    )

    val meta = profile(
      "metaDataCreate",
      prevFetch match {
        case None      => FileMetaData(attributes, "", Instant.now, Instant.now, Instant.now)
        case Some(old) => FileMetaData(attributes, "", old.firstFetched, old.lastModified, Instant.now)
      },
    )

    errOrBytes match {
      case Failure(exception) =>
        logger.error(
          s"fetching ${file.path()} from offset ${offsetToReadFrom} to ${offsetToReadFrom + sliceSize} -EXCEPTION",
          exception,
        )
        throw exception
      case Success(Some(bytes)) =>
        logger.info(
          s"fetching ${file.path()} from offset ${offsetToReadFrom} to ${offsetToReadFrom + sliceSize} - BYTES",
        )
        Option((prevFetch, FetchedFile(meta, bytes)))
      case Success(None) =>
        logger.info(
          s"fetching ${file.path()} from offset ${offsetToReadFrom} to ${offsetToReadFrom + sliceSize} - EMPTY",
        )
        Option.empty
    }
  }

  // translates a MonitoredDirectory and previously known FileMetaData into a FileMetaData and FileBody
  def handleFetchedFile(
    tail:      Boolean,
    prevFetch: Option[FileMetaData],
    current:   FetchedFile,
  ): (FileMetaData, FileBody) =
    prevFetch match {
      case Some(previously)
          if previously.attribs.size != current.meta.attribs.size || previously.hash != current.meta.hash =>
        // file changed in size and/or hash
        logger.info(s"fetched ${current.meta.attribs.path}, it was known before and it changed")
        if (tail) {
          if (current.meta.attribs.size > previously.attribs.size) {
            val hashPrevBlock =
              DigestUtils.sha256Hex(util.Arrays.copyOfRange(current.body, 0, previously.attribs.size.toInt))
            if (previously.hash == hashPrevBlock) {
              logger.info(
                s"tail ${current.meta.attribs.path} [${previously.attribs.size.toInt}, ${current.meta.attribs.size.toInt})",
              )
              val tail =
                util.Arrays.copyOfRange(current.body, previously.attribs.size.toInt, current.meta.attribs.size.toInt)
              (current.meta.inspectedNow().modifiedNow(), FileBody(tail, previously.attribs.size))
            } else {
              logger.warn(
                s"the tail of ${current.meta.attribs.path} is to be followed, but previously seen content changed. we'll provide the entire file.",
              )
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

  def bySlicesHandleFetchedFile(
    mode:      MonitorMode,
    prevFetch: Option[FileMetaData],
    current:   FetchedFile,
  ): (FileMetaData, FileBody) =
    prevFetch match {
      case Some(previous) if mode == MonitorMode.Update && current.meta.hasChangedSince(previous) =>
        logger.info(
          s"distant file was modified since last fetch, previously timestamp=${previous.attribs.timestamp}, current timestamp=${current.meta.attribs.timestamp}",
        )
        val nextOffsetToReadFrom = current.body.length.toLong
        (current.meta.inspectedNow().offset(nextOffsetToReadFrom), FileBody(current.body, 0))
      case Some(previous) =>
        logger.info(
          s"fetched ${current.meta.attribs.path}, bytes read= ${current.body.length} , from offset=${previous.offset}",
        )
        val nextOffsetToReadFrom = previous.offset + current.body.length
        (current.meta.inspectedNow().offset(nextOffsetToReadFrom), FileBody(current.body, nextOffsetToReadFrom))
      case None =>
        // slice is new
        logger.info(s"fetched ${current.meta.attribs.path}, bytes read= ${current.body.length} , from offset=0")
        val nextOffsetToReadFrom = current.body.length.toLong
        (current.meta.inspectedNow().offset(nextOffsetToReadFrom), FileBody(current.body, nextOffsetToReadFrom))
    }

  def listFilesToFetchFrom(monitoredPath: MonitoredPath): Seq[AbsoluteFtpFile] =
    profile(
      "listFilesToFetchFrom",
      FtpFileLister(ftp).listFiles(monitoredPath.p.toString).filter(f =>
        !MaxAge.minus(f.age()).isNegative && f.name().matches(settings.filter),
      ),
    )

  // fetches files from a monitored directory when needed
  def fetchFromMonitoredPlaces(monitoredPath: MonitoredPath): LazyList[(FileMetaData, FileBody)] =
    profile("fetchFromMonitoredPlaces", fetchFromFiles(listFilesToFetchFrom(monitoredPath), monitoredPath.mode))

  def fetchFromFiles(files: Seq[AbsoluteFtpFile], mode: MonitorMode): LazyList[(FileMetaData, FileBody)] =
    profile(
      "fetchFromFiles",
      if (!isBySlices) {
        files
          .to(LazyList)
          // Get the metadata from the offset store
          .map(file => profile("mapFileConverter1", (file, fileConverter.getFileOffset(file.path()))))
          // Filter out the files that do not need to be fetched
          .filter { case (file, prevFetch) => profile("requiresFetch", requiresFetch(file, prevFetch)) }
          // Fetch the latest file contents
          .flatMap { case (file, prevFetch) => profile("fetchFile", fetch(file, prevFetch)) }
          // Extract the file changes depending on the tracking mode
          .map {
            case (prevFile, currentFile) =>
              profile("handleFetchedFile", handleFetchedFile(mode == MonitorMode.Tail, prevFile, currentFile))
          }
      } else {
        files
          .to(LazyList)
          // Get the metadata from the offset store
          .map(file => (file, profile("mapFileConverter2", fileConverter.getFileOffset(file.path()))))
          // Filter out the files that do not need to be fetched
          .filter { case (file, prevFetch) => profile("bySlicesRequiresFetch", bySlicesRequiresFetch(file, prevFetch)) }
          // Fetch the latest file contents
          .flatMap { case (file, prevFetch) => profile("bySlicesFetch", bySlicesFetch(mode, file, prevFetch)) }
          // Extract the file changes depending on the tracking mode
          .map {
            case (prevFile, currentFile) =>
              profile("bySlicesHandleFetchedFile", bySlicesHandleFetchedFile(mode, prevFile, currentFile))
          }
      },
    )

  def connectFtp(): Try[FTPClient] = {
    ftp.disconnect() // TODO: maybe we should keep the connection open. However, the underlying ftp client is a major PITA and this is way easier.
    if (!ftp.isConnected) {
      ftp.setConnectTimeout(settings.timeoutMs)
      ftp.setDefaultTimeout(settings.timeoutMs)
      ftp.setDataTimeout(Duration.ofMillis(settings.timeoutMs.toLong))
      ftp.setRemoteVerificationEnabled(false)
      ftp.addProtocolCommandListener(
        new ProtocolCommandListener {
          override def protocolCommandSent(e: ProtocolCommandEvent): Unit =
            logger.trace(s">> ${e.getCommand} ${e.getMessage} ${e.getReplyCode} ${e.isCommand} ${e.isReply}")

          override def protocolReplyReceived(e: ProtocolCommandEvent): Unit =
            logger.trace(s"<< ${e.getCommand} ${e.getMessage} ${e.getReplyCode} ${e.isCommand} ${e.isReply}")
        },
      )

      logger.info(s"connect ${settings.host}:${settings.port}")
      settings.port match {
        case Some(explicitPort) => ftp.connect(settings.host, explicitPort)
        case None               => ftp.connect(settings.host)
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
      ftp match {
        case client: FTPSClient =>
          logger.info("FTPS Connection needed, setting appropriate settings")
          client.execPBSZ(0)
          client.execPROT("P")
        case _ =>
      }
      ftp.enterLocalPassiveMode()
      ftp.setFileType(FTP.BINARY_FILE_TYPE)
      ftp.setControlKeepAliveTimeout(Duration.ofSeconds(15)) //send NOOP every [seconds]
    }
    Success(ftp)
  }

  def poll(): Try[LazyList[(FileMetaData, FileBody, MonitoredPath)]] = connectFtp() match {
    case Success(_) =>
      Try(settings.directories.to(LazyList).flatMap(dir =>
        fetchFromMonitoredPlaces(dir).map { case (meta, body) => (meta, body, dir) },
      ))
    case Failure(err) => logger.warn(s"cannot connect to ftp: ${err.toString}")
      Failure(err)
  }
}

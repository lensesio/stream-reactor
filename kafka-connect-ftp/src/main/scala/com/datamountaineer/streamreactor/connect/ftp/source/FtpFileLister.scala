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

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.net.ftp.{FTPClient, FTPFile}

import java.nio.file.{FileSystems, Paths}
import java.time.{Duration, Instant}

// org.apache.commons.net.ftp.FTPFile only contains the relative path
case class AbsoluteFtpFile(ftpFile:FTPFile, parentDir:String) {
  def name() = ftpFile.getName
  def size() = ftpFile.getSize
  def timestamp() = ftpFile.getTimestamp.toInstant
  def path() = Paths.get(parentDir, name).toString
  def age(): Duration = Duration.between(timestamp, Instant.now)
}

case class FtpFileLister(ftp: FTPClient) extends StrictLogging {

  def pathMatch(pattern: String, path: String):Boolean = {
    val g = s"glob:$pattern"
    FileSystems.getDefault.getPathMatcher(g).matches(Paths.get(path))
  }

  def isGlobPattern(pattern: String): Boolean = List("*", "?", "[", "{").exists(pattern.contains(_))

  def listFiles(path: String) : Seq[AbsoluteFtpFile] = {
    val pathParts : Seq[String] = path.split("/")

    val (basePath, patterns) = pathParts.zipWithIndex.view.find{case (part, _) => isGlobPattern(part)} match {
      case Some((_, index)) => pathParts.splitAt(index)
      case _ => (pathParts.init, Seq[String](pathParts.last))
    }

    def iter(basePath: String, patterns: List[String]) : Seq[AbsoluteFtpFile] = {
      Option(ftp.listFiles(basePath + "/")) match {
        case Some(files) => patterns match {
          case pattern :: Nil => {
            files.filter(f => f.isFile && pathMatch(pattern, f.getName))
              .map(AbsoluteFtpFile(_, basePath + "/"))
          }
          case pattern :: rest => {
            files.filter(f => f.getName() != "." && f.getName() != ".." && pathMatch(pattern, f.getName))
              .flatMap(f => iter(Paths.get(basePath, f.getName).toString, rest))
          }
          case _ => Seq()
        }
        case _ => Seq()
      }
    }

    iter(Paths.get("/", basePath:_*).toString, patterns.toList)
  }
}

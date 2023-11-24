/*
 * Copyright 2017-2023 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.ftp.source
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.net.ftp.FTPFile
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RegExTest extends AnyFunSuite with Matchers with BeforeAndAfter with StrictLogging with MockitoSugar {
  def mockFile(name: String) = {
    val f = mock[FTPFile]
    when(f.isFile).thenReturn(true)
    when(f.isDirectory).thenReturn(false)
    when(f.getName()).thenReturn(name)
    f
  }
  test("Matches RegEx") {
    FtpSourceConfig.fileFilter -> ".*"

    val f: source.AbsoluteFtpFile = new AbsoluteFtpFile(mockFile("file.txt"), "\\");
    f.name().matches(".*") shouldBe true
    f.name().matches("a") shouldBe false
  }
}

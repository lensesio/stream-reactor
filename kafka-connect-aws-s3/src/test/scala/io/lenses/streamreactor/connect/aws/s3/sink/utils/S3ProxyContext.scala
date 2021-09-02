
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.utils

import com.typesafe.scalalogging.LazyLogging
import org.eclipse.jetty.util.component.AbstractLifeCycle
import org.gaul.s3proxy.S3Proxy
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext

import java.net.URI
import java.util.Properties

object S3ProxyContext {

  val Port: Int = 8099
  val Identity: String = "identity"
  val Credential: String = "credential"
  val Uri: String = "http://127.0.0.1:" + Port

  val TestBucket: String = "employees"
}

class S3ProxyContext extends LazyLogging {

  import S3ProxyContext._

  private val proxy: S3Proxy = createProxy

  def createProxy: S3Proxy = {

    val properties = new Properties()
    properties.setProperty("jclouds.filesystem.basedir", "/tmp/blobstore")

    val context: BlobStoreContext = ContextBuilder
      .newBuilder("filesystem")
      .credentials(Identity, Credential)
      .overrides(properties)
      .build(classOf[BlobStoreContext])

    S3Proxy.builder
      .blobStore(context.getBlobStore)
      .endpoint(URI.create(Uri))
      .build

  }

  def startProxy: S3Proxy = {

    proxy.start()
    while ( {
      !proxy.getState.equals(AbstractLifeCycle.STARTED)
    }) Thread.sleep(1)

    proxy
  }

  def stopProxy: S3Proxy = {
    proxy.stop()
    while ( {
      !proxy.getState.equals(AbstractLifeCycle.STOPPED)
    }) Thread.sleep(1)

    proxy
  }

}

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
package io.lenses.streamreactor.connect.http.sink.tpl.templates

import com.github.mustachejava.DefaultMustacheFactory
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectObjectHandler

import java.io.StringReader

case class RawTemplate(
  endpoint: String,
  content:  String,
  headers:  Seq[(String, String)],
) {
  def parse(): Template = {
    val mf = new DefaultMustacheFactory()
    mf.setObjectHandler(new KafkaConnectObjectHandler())

    val endpointTemplate = mf.compile(new StringReader(endpoint), "endpoint")
    val contentTemplate  = mf.compile(new StringReader(content), "content")
    val headerTemplates = headers.zipWithIndex.map {
      case ((tplKey, tplVal), idx) =>
        mf.compile(new StringReader(tplKey), s"headerKey_$idx") -> mf.compile(new StringReader(tplVal),
                                                                              s"headerVal_$idx",
        )
    }
    Template(endpointTemplate, contentTemplate, headerTemplates)
  }
}

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

import com.github.mustachejava.Mustache
import org.apache.kafka.connect.sink.SinkRecord

import java.io.StringWriter

case class Template(
  endpointTemplate: Mustache,
  contentTemplate:  Mustache,
  headerTemplates:  Seq[(Mustache, Mustache)],
) {
  def process(
    sinkRecord: SinkRecord,
  ): ProcessedTemplate =
    ProcessedTemplate(
      executeTemplate(endpointTemplate, sinkRecord),
      executeTemplate(contentTemplate, sinkRecord),
      headerTemplates.map {
        case (keyTpl, valTpl) =>
          executeTemplate(keyTpl, sinkRecord) -> executeTemplate(valTpl, sinkRecord)
      },
    )

  private def executeTemplate(
    template:   Mustache,
    sinkRecord: SinkRecord,
  ): String = {
    val stringWriter = new StringWriter()
    val newWriter    = template.execute(stringWriter, sinkRecord)
    // really these writers are the same but checking the writer ref
    // that is returned helps with testing.
    newWriter.flush()
    newWriter.toString
  }

}

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

import org.apache.kafka.connect.source.SourceRecord

import java.util
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 31/08/2017. 
  * Default (Template) Project
  */
class NopSourceRecordConverter extends SourceRecordConverter{
  override def configure(props: util.Map[String, _]): Unit = {}

  override def convert(in: SourceRecord): util.List[SourceRecord] = Seq(in).asJava
}

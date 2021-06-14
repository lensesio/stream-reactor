/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.WRITE_MODE
import io.lenses.streamreactor.connect.aws.s3.model.S3OutputStreamOptions

trait S3WriteModeSettings extends BaseSettings {

  def s3WriteOptions(props: Map[String,String]) : Either[Exception,S3OutputStreamOptions] = {
    S3OutputStreamOptions(getString(WRITE_MODE), props)
  }

}

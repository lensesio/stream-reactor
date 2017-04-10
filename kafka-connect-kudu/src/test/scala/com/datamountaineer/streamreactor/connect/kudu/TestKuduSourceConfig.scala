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

package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.kudu.config.{KuduSinkConfig, KuduSinkConfigConstants}

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * stream-reactor
  */
class TestKuduSourceConfig extends TestBase {
  "A KuduSinkConfig should return Kudu Master" in {
    val config = new KuduSinkConfig(getConfig)
    config.getString(KuduSinkConfigConstants.KUDU_MASTER) shouldBe KUDU_MASTER
    config.getString(KuduSinkConfigConstants.EXPORT_ROUTE_QUERY) shouldBe EXPORT_MAP
  }
}
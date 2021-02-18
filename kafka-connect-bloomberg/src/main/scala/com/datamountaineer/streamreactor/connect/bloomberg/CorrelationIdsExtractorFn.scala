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

package com.datamountaineer.streamreactor.connect.bloomberg

import com.bloomberglp.blpapi.SubscriptionList

import scala.collection.JavaConverters._

object CorrelationIdsExtractorFn {
  /**
    * Given a list of Bloomberg subscriptions builds a list of all the correlation ids.
    *
    * @param subscriptions : A list of Bloomberg subscriptions
    * @return A string listing all the correlation ids associated with the input parameter
    */
  def apply(subscriptions: SubscriptionList) : String = {
    if (subscriptions != null) {
      subscriptions.asScala.map(_.correlationID().value()).mkString(",")
    }
    else {
      ""
    }
  }
}

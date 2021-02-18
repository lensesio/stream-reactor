/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.config

import com.datamountaineer.streamreactor.common.config.Helpers
import com.datamountaineer.streamreactor.connect.TestUtilsBase
import org.apache.kafka.common.config.ConfigException

/**
  * Created by andrew@datamountaineer.com on 23/08/2017. 
  * kafka-connect-common
  */
class TestHelpers extends TestUtilsBase  {

  val kcqlConstant: String = "myconnector.kcql"

  "should throw exception if topics not specified in connector props" in {
    val props = Map("topics" -> "t1",
      s"$kcqlConstant" -> "insert into table select  * from t1;insert into table2 select * from t2"
    )

    intercept[ConfigException] {
      Helpers.checkInputTopics(kcqlConstant, props)
    }

  }

  "should throw exception if topics not specified in kcql" in {
    val props = Map("topics" -> "t1,t2",
      s"$kcqlConstant" -> "insert into table select  * from t1"
    )

    intercept[ConfigException] {
      Helpers.checkInputTopics(kcqlConstant, props)
    }
  }

  "should not throw exception if all good" in {
    val props = Map("topics" -> "t1,t2",
      s"$kcqlConstant" -> "insert into table select  * from t1;insert into table2 select * from t2"
    )

    val res = Helpers.checkInputTopics(kcqlConstant, props)
    res shouldBe true
  }

  "should add topics involved in kcql error to message" in {
    val props = Map("topics" -> "topic1",
      s"$kcqlConstant" -> "insert into table select time,c1,c2 from topic1 WITH TIMESTAMP time"
    )

    val e = intercept[ConfigException] {
      Helpers.checkInputTopics(kcqlConstant, props)
    }

    e.getMessage.contains("topic1WITHTIMESTAMPtime") shouldBe true
  }
}

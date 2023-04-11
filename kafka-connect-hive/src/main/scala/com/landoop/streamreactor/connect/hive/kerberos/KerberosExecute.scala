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
package com.landoop.streamreactor.connect.hive.kerberos

trait KerberosExecute {
  def execute[T](kerberosLogin: Option[KerberosLogin])(thunk: => T): T = kerberosLogin.fold(thunk)(_.run(thunk))
}

trait UgiExecute {
  def execute[T](thunk: => T): T
}

object UgiExecute {
  val NoOp = new UgiExecute {
    override def execute[T](thunk: => T): T = thunk
  }
}

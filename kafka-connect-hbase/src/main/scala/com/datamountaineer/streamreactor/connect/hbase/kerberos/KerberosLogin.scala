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
package io.lenses.streamreactor.connect.hbase.kerberos

import java.io.IOException
import java.net.InetAddress
import java.security.PrivilegedAction

import io.lenses.streamreactor.connect.hbase.kerberos.utils.AsyncFunctionLoop
import com.typesafe.scalalogging.StrictLogging
import javax.security.auth.login.LoginContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation

import scala.concurrent.duration.Duration
import scala.concurrent.duration._

sealed trait KerberosLogin extends AutoCloseable {
  def run[T](thunk: => T): T
}

case class UserPasswordLogin(ugi: UserGroupInformation, interval: Duration, lc: LoginContext)
    extends KerberosLogin
    with StrictLogging {

  private val asyncTicketRenewal = new AsyncFunctionLoop(interval, "Kerberos")(renewKerberosTicket())
  asyncTicketRenewal.start()

  override def close(): Unit = {
    asyncTicketRenewal.close()
    lc.logout()
  }

  private def renewKerberosTicket(): Unit =
    try {
      ugi.reloginFromTicketCache()
    } catch {
      case e: IOException =>
        // We ignore this exception during relogin as each successful relogin gives
        // additional 24 hours of authentication in the default config. In normal
        // situations, the probability of failing relogin 24 times is low and if
        // that happens, the task will fail eventually.
        logger.error("Error renewing the Kerberos ticket", e)
    }
  override def run[T](thunk: => T): T =
    ugi.doAs(new PrivilegedAction[T] {
      override def run(): T = thunk
    })
}

case class KeytabLogin(ugi: UserGroupInformation, interval: Duration) extends KerberosLogin {
  private val logger             = org.slf4j.LoggerFactory.getLogger(getClass.getName)
  private val asyncTicketRenewal = new AsyncFunctionLoop(interval, "Kerberos")(renewKerberosTicket())
  asyncTicketRenewal.start()

  private def renewKerberosTicket(): Unit =
    try {
      ugi.reloginFromKeytab()
    } catch {
      case e: IOException =>
        // We ignore this exception during relogin as each successful relogin gives
        // additional 24 hours of authentication in the default config. In normal
        // situations, the probability of failing relogin 24 times is low and if
        // that happens, the task will fail eventually.
        logger.error("Error renewing the Kerberos ticket", e)
    }
  override def close(): Unit = asyncTicketRenewal.close()
  override def run[T](thunk: => T): T =
    ugi.doAs(new PrivilegedAction[T] {
      override def run(): T = thunk
    })
}

object KerberosLogin {

  def from(kerberos: Kerberos, configuration: Configuration): KerberosLogin =
    kerberos.auth match {
      case Left(settings)  => from(settings, kerberos.ticketRenewalMs.millis, configuration)
      case Right(settings) => from(settings, kerberos.ticketRenewalMs.millis, configuration)
    }

  def from(settings: KeytabSettings, interval: Duration, configuration: Configuration): KeytabLogin = {
    UserGroupInformation.setConfiguration(configuration)

    val hostname = InetAddress.getLocalHost.getCanonicalHostName
    // replace the _HOST specified in the principal config to the actual host
    val principal = SecurityUtil.getServerPrincipal(settings.principal, hostname)

    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, settings.keytab)

    KeytabLogin(ugi, interval)
  }

  def from(settings: UserPasswordSettings, interval: Duration, configuration: Configuration): UserPasswordLogin = {
    UserGroupInformation.setConfiguration(configuration)

    val lc = new LoginContext(settings.jaasEntryName, new UserPassCallbackHandler(settings.user, settings.password))
    lc.login()

    val subject = lc.getSubject
    UserGroupInformation.loginUserFromSubject(subject)
    val ugi = UserGroupInformation.getCurrentUser

    UserPasswordLogin(ugi, interval, lc)
  }
}

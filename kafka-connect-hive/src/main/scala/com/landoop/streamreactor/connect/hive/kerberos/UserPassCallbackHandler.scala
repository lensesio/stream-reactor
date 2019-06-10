package com.landoop.streamreactor.connect.hive.kerberos

import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback

class UserPassCallbackHandler(user: String, password: String) extends CallbackHandler {
  override def handle(callbacks: Array[Callback]): Unit = {
    callbacks.foreach {
      case nc: NameCallback => nc.setName(user)
      case pc: PasswordCallback => pc.setPassword(password.toCharArray())
      case _ =>
    }
  }
}

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
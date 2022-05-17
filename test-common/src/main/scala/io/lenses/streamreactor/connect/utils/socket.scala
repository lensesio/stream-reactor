package io.lenses.streamreactor.connect.utils

import java.net.ServerSocket

object socket {

  def findFreePort: Int = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(true)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}

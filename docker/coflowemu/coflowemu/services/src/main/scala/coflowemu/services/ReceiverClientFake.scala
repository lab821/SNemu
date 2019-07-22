package coflowemu.services

import coflowemu.util.AkkaUtils
import coflowemu.{Logging, Utils}
import coflowemu.framework.client._
import coflowemu.framework._

private[coflowemu] object ReceiverClientFake {

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("USAGE: ReceiverClientFake <masterUrl> <coflowId> [dataName]")
      System.exit(1)
    }
    
    val url = args(0)
    val coflowId = args(1)
    val DATA_NAME = if (args.length > 2) args(2) else "DATA"

    val listener = new TestListener
    val client = new CoflowemuClient("2", url, listener)
    client.start()
    
    Thread.sleep(5000)
    
    println("Trying to retrieve " + DATA_NAME)
    client.getFake(DATA_NAME, coflowId)
    println("Got " + DATA_NAME + ". Now waiting to die.")
    
    client.awaitTermination()
  }
}

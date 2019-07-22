package coflowemu.services

import coflowemu.util.AkkaUtils
import coflowemu.{Logging, Utils}
import coflowemu.framework.client._
import coflowemu.framework._

private[coflowemu] object ReceiverClientFile {

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
      println("USAGE: ReceiverClientFile <masterUrl> <coflowId> [fileName]")
      System.exit(1)
    }
    
    val url = args(0)
    val coflowId = args(1)
    val FILE_NAME = if (args.length > 2) args(2) else "INFILE"

    val listener = new TestListener
    val client = new CoflowemuClient("ReceiverClientFile", url, listener)
    client.start()
    
    Thread.sleep(5000)
    
    println("Trying to retrieve " + FILE_NAME)
    client.getFile(FILE_NAME, coflowId)
    println("Got " + FILE_NAME + ". Now waiting to die.")
    
    client.awaitTermination()
  }
}

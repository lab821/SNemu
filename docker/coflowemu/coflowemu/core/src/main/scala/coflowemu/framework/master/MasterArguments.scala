package coflowemu.framework.master

import coflowemu.Utils
import coflowemu.util.IntParam
import java.util.Properties

/**
 * Command-line parser for the master.
 */
private[coflowemu] class MasterArguments(
  args: Array[String]) {

  var ip = Utils.localIpAddress
  var port = 1606
  var webUiPort = 16016
  var traceFile = getProperty("traceFile")
  var numNodes = 0
  
  // Check for settings in environment variables 
  if (System.getenv("VARYS_MASTER_IP") != null) {
    ip = System.getenv("VARYS_MASTER_IP")
  }
  if (System.getenv("VARYS_MASTER_PORT") != null) {
    port = System.getenv("VARYS_MASTER_PORT").toInt
  }
  if (System.getenv("VARYS_MASTER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("VARYS_MASTER_WEBUI_PORT").toInt
  }
  
  parse(args.toList)

  def parse(args: List[String]): Unit = args match {
    case ("--ip" | "-i") :: value :: tail =>
      ip = value
      parse(tail)

    case ("--trace" | "-t") :: value :: tail =>
      traceFile = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case Nil => {}

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Master [options]\n" +
      "\n" +
      "Options:\n" +
      "  -i IP, --ip IP         IP address or DNS name to listen on\n" +
      "  -p PORT, --port PORT   Port to listen on (default: 1606)\n" +
      "  --webui-port PORT      Port for web UI (default: 16016)\n" +
      "  -t --trace TRACEPATH   Path to trace file")
    System.exit(exitCode)
  }

  def getProperty(name : String): String ={
    val prop = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("trace.properties")
    prop.load(in)
    prop.getProperty(name)
  }
}

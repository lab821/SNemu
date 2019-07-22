package coflowemu.framework.master.ui

import akka.actor._

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import scala.concurrent.duration.Duration

import coflowemu.{Logging, Utils}
import coflowemu.ui.JettyUtils
import coflowemu.ui.JettyUtils._

/**
 * Web UI server for the standalone master.
 */
private[coflowemu]
class MasterWebUI(masterActorRef_ : ActorRef, requestedPort: Int) extends Logging {
  implicit val timeout = Duration.create(
    System.getProperty("coflowemu.akka.askTimeout", "10").toLong, "seconds")
  val host = Utils.localIpAddress
  val port = requestedPort

  val masterActorRef = masterActorRef_
  
  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val coflowPage = new CoflowPage(this)
  val indexPage = new IndexPage(this)

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort.get))
    } catch {
      case e: Exception =>
        logError("Failed to create Master JettyUtils", e)
        System.exit(1)
    }
  }

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)),
    ("/coflow/json", (request: HttpServletRequest) => coflowPage.renderJson(request)),
    ("/coflow", (request: HttpServletRequest) => coflowPage.render(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def stop() {
    server.foreach(_.stop())
  }
}

private[coflowemu] object MasterWebUI {
  val STATIC_RESOURCE_DIR = "coflowemu/ui/static"
}

package coflowemu.util

import akka.actor._
import akka.pattern.ask

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.Await

import coflowemu.CoflowemuException

/**
 * Various utility classes for working with Akka.
 */
private[coflowemu] object AkkaUtils {

  val AKKA_TIMEOUT_MS: Int = System.getProperty("coflowemu.akka.timeout", "30").toInt * 1000

  /**
   * Creates an ActorSystem ready for remoting, with various Varys features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   */
  def createActorSystem(name: String, host: String, port: Int): (ActorSystem, Int) = {
    val akkaThreads = System.getProperty("coflowemu.akka.threads", "4").toInt
    val akkaBatchSize = System.getProperty("coflowemu.akka.batchSize", "15").toInt
    val akkaTimeout = System.getProperty("coflowemu.akka.timeout", "60").toInt
    val akkaFrameSize = System.getProperty("coflowemu.akka.frameSize", "10").toInt * 1048576
    val logLevel = System.getProperty("coflowemu.akka.logLevel", "ERROR")
    val lifecycleEvents = if (System.getProperty("coflowemu.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"
    val logRemoteEvents = if (System.getProperty("coflowemu.akka.logRemoteEvents", "false").toBoolean) "on" else "off"
    val akkaWriteTimeout = System.getProperty("coflowemu.akka.writeTimeout", "30").toInt

    val akkaConf = ConfigFactory.parseString("""
      akka {
        daemonic = on
        jvm-exit-on-fatal-error = off
        loggers = ["akka.event.slf4j.Slf4jLogger"]
        extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]

        actor {
          debug {
            # receive = on
            # autoreceive = on
            # lifecycle = on
            # fsm = on
            # event-stream = on
          }
         
          provider = "akka.remote.RemoteActorRefProvider"

          serializers {  
            java = "akka.serialization.JavaSerializer"
            kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
          }

          serialization-bindings {
            "coflowemu.framework.FrameworkMessage" = kryo
            "java.io.Serializable" = java
          }

          # Details of configuration params is at https://github.com/romix/akka-kryo-serialization
          kryo {
            type = "graph"  
            idstrategy = "incremental"  

            # Define a default size for serializer pool
            # Try to define the size to be at least as big as the max possible number
            # of threads that may be used for serialization, i.e. max number
            # of threads allowed for the scheduler
            serializer-pool-size = 32

            # Define a default size for byte buffers used during serialization
            buffer-size = 65536  

            use-manifests = false
            implicit-registration-logging = false 
            kryo-trace = false

            # classes that will be serialized aka the messages to be sent
            classes = [
              "coflowemu.framework.RegisterSlave",
              "coflowemu.framework.Heartbeat",
              "coflowemu.framework.RegisteredSlave",
              "coflowemu.framework.RegisterSlaveFailed",
              "coflowemu.framework.RegisterClient",
              "coflowemu.framework.RegisterCoflow",
              "coflowemu.framework.RegisterCoflowFailed",
              "coflowemu.framework.RejectedCoflow",
              "coflowemu.framework.UnregisterCoflow",
              "coflowemu.framework.RequestBestRxMachines",
              "coflowemu.framework.RequestBestTxMachines",
              "coflowemu.framework.RegisteredClient",
              "coflowemu.framework.CoflowKilled",
              "coflowemu.framework.RegisterClientFailed",
              "coflowemu.framework.RegisteredCoflow",
              "coflowemu.framework.UnregisteredCoflow",
              "coflowemu.framework.BestRxMachines",
              "coflowemu.framework.BestTxMachines",
              "coflowemu.framework.UpdatedRates",
              "coflowemu.framework.AddFlow",
              "coflowemu.framework.AddFlows",
              "coflowemu.framework.GetFlow",
              "coflowemu.framework.GetFlows",
              "coflowemu.framework.FlowProgress",
              "coflowemu.framework.DeleteFlow",
              "coflowemu.framework.GotFlowDesc",
              "coflowemu.framework.GotFlowDescs",
              "coflowemu.framework.CoflowDescription",
              "coflowemu.framework.CoflowType$",
              "coflowemu.framework.FlowDescription",
              "[Lcoflowemu.framework.FlowDescription;",
              "coflowemu.framework.FileDescription",
              "coflowemu.framework.ObjectDescription",
              "coflowemu.framework.DataIdentifier",
              "coflowemu.framework.DataType$",
              "scala.collection.immutable.Map$Map1",
              "scala.collection.immutable.Map$Map2",
              "scala.collection.immutable.Map$Map3",
              "scala.collection.immutable.Map$Map4",
              "scala.collection.immutable.HashMap$HashTrieMap",
              "coflowemu.framework.StartEmuInTime",
              "coflowemu.framework.LaunchTask"
            ]  
          }
        }
      }

      akka.loglevel = "%s"
      akka.stdout-loglevel = "%s"
      akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      akka.remote.netty.tcp.hostname = "%s"
      akka.remote.netty.tcp.port = %d
      akka.remote.netty.tcp.tcp-nodelay = on
      akka.remote.netty.tcp.connection-timeout = %ds
      akka.remote.netty.tcp.maximum-frame-size = %dB
      akka.remote.netty.tcp.execution-pool-size = %d
      akka.actor.default-dispatcher.throughput = %d
      akka.remote.log-remote-lifecycle-events = %s
      akka.remote.log-sent-messages = %s
      akka.remote.log-received-messages = %s
      akka.remote.netty.write-timeout = %ds
      """.format(
        logLevel, 
        logLevel, 
        host, 
        port, 
        akkaTimeout, 
        akkaFrameSize, 
        akkaThreads, 
        akkaBatchSize,
        lifecycleEvents, 
        logRemoteEvents, 
        logRemoteEvents, 
        akkaWriteTimeout))

    val actorSystem = ActorSystem(name, akkaConf)

    // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
    // hack because Akka doesn't let you figure out the port through the public API yet.
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    return (actorSystem, boundPort)
  }

  /** 
   * Send a one-way message to an actor, to which we expect it to reply with true. 
   */
  def tellActor(actor: ActorRef, message: Any) {
    if (!askActorWithReply[Boolean](actor, message)) {
      throw new CoflowemuException(actor + " returned false, expected true.")
    }
  }

  /**
   * Send a message to an actor and get its result within a default timeout, or
   * throw a CoflowemuException if this fails.
   */
  def askActorWithReply[T](actor: ActorRef, message: Any, timeout: Int = AKKA_TIMEOUT_MS): T = {
    if (actor == null) {
      throw new CoflowemuException("Error sending message as the actor is null " + "[message = " +
        message + "]")
    }
    
    try {
      val future = actor.ask(message)(timeout.millis)
      val result = Await.result(future, timeout.millis)
      if (result == null) {
        throw new Exception(actor + " returned null")
      }
      return result.asInstanceOf[T]
    } catch {
      case ie: InterruptedException => throw ie
      case e: Exception => {
        throw new CoflowemuException(
          "Error sending message to " + actor + " [message = " + message + "]", e)
      }
    }
  }

  def getActorRef(url: String, context: ActorContext): ActorRef = {
    val timeout = AKKA_TIMEOUT_MS.millis
    Await.result(context.actorSelection(url).resolveOne(timeout), timeout)
  }  
}

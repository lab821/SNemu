package coflowemu.framework.master

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.routing._
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.io._
import coflowemu.framework._
import coflowemu.framework.master.scheduler._
import coflowemu.framework.master.ui.MasterWebUI
import coflowemu.{Logging, Utils, CoflowemuException}
import coflowemu.util.{AkkaUtils, SlaveToBpsMap}

import scala.collection.mutable

/**
  * Created by Nicknameless on 2018/9/26.
  */
private[coflowemu] class EmulationMaster(systemName:String,
                                         actorName: String,
                                         host: String,
                                         port: Int,
                                         webUiPort: Int,
                                         traceFile: String) extends Master(systemName, actorName, host, port, webUiPort){

  val traceInfo = TraceInfo.parseTrace(traceFile)//read trace info
  var emuNodeNumCount = new AtomicInteger(traceInfo.traceHostNum)
  val idToCoflowTrace = new mutable.HashMap[String, TraceCoflowInfo]()
  //val idToTraceCoflow = new ConcurrentHashMap[String, CoflowDescription]()
  val idToEmuCoflow = new ConcurrentHashMap[String, CoflowDescription]()

//  override var nextClientNumber = new AtomicInteger()
//  override val idToClient = new ConcurrentHashMap[String, ClientInfo]()
//  override val actorToClient = new ConcurrentHashMap[ActorRef, ClientInfo]
//  override val addressToClient = new ConcurrentHashMap[Address, ClientInfo]


  override def start(): (ActorSystem, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(
      Props(new EmulationMasterActor(host, boundPort, webUiPort)).withRouter(
        RoundRobinRouter(nrOfInstances = NUM_MASTER_INSTANCES)),
      name = actorName)
    (actorSystem, boundPort)
  }


  private def now() = System.currentTimeMillis

  private[coflowemu] class EmulationMasterActor(ip: String,
                                                port: Int,
                                                webUiPort: Int) extends MasterActor(ip, port,webUiPort){
    override def receive = {
      case RegisterSlave(id, host, slavePort, slave_webUiPort, slave_commPort, publicAddress) => {
        val currentSender = sender
        logInfo("Registering slave %s:%d".format(host, slavePort))
        if (idToSlave.containsKey(id)) {
          currentSender ! RegisterSlaveFailed("Duplicate slave ID")
        } else {
          addSlave(
            id,
            host,
            slavePort,
            slave_webUiPort,
            slave_commPort,
            publicAddress,
            currentSender)

          // Wait for webUi to bind. Needed when NUM_MASTER_INSTANCES > 1.
          while (webUi.boundPort == None) {
            Thread.sleep(100)
          }

          // context.watch doesn't work with remote actors but helps for testing
          // context.watch(currentSender)
          currentSender ! RegisteredSlave("http://" + masterPublicAddress + ":" + webUi.boundPort.get)
        }
      }

      case RegisterClient(clientName, host, commPort) => {
        val currentSender = sender
        val st = now
        logTrace("Registering client %s@%s:%d".format(clientName, host, commPort))

        if (hostToSlave.containsKey(host)) {
          val client = addClient(clientName, host, commPort, currentSender)

          // context.watch doesn't work with remote actors but helps for testing
          // context.watch(currentSender)
          val slave = hostToSlave(host)
          currentSender ! RegisteredClient(
            client.id,
            slave.id,
            "coflowemu://" + slave.host + ":" + slave.port)

          logInfo("Registered client " + clientName + " with ID " + client.id + " in " +
            (now - st) + " milliseconds")

          val emuNodeRemained = emuNodeNumCount.decrementAndGet()
          if(emuNodeRemained == 0){
            logInfo("All clients are registered, the emulation process is starting.")
            self ! StartEmu
          } else {
            logInfo(emuNodeRemained.toString + " emulation node(s) remain to be registered.")
          }
        } else {
          currentSender ! RegisterClientFailed("No Coflowemu slave at " + host)
        }
      }

      case RegisterCoflow(clientId, description) => {
        val currentSender = sender
        val st = now
        logTrace("Registering coflow " + description.name)

        if (CONSIDER_DEADLINE && description.deadlineMillis == 0) {
          currentSender ! RegisterCoflowFailed("Must specify a valid deadline")
        } else {
          val client = idToClient.get(clientId)
          if (client == null) {
            currentSender ! RegisterCoflowFailed("Invalid clientId " + clientId)
          } else {
            val coflow = addCoflow(client, description, currentSender)

            // context.watch doesn't work with remote actors but helps for testing
            // context.watch(currentSender)
            currentSender ! RegisteredCoflow(coflow.id)
            logInfo("Registered coflow " + description.name + " with ID " + coflow.id + " in " +
              (now - st) + " milliseconds")
          }
        }
      }

      case UnregisterCoflow(coflowId) => {
        removeCoflow(idToCoflow.get(coflowId))
        sender ! true
      }

      case Heartbeat(slaveId, newRxBps, newTxBps) => {
        val slaveInfo = idToSlave.get(slaveId)
        if (slaveInfo != null) {
          slaveInfo.updateNetworkStats(newRxBps, newTxBps)
          slaveInfo.lastHeartbeat = System.currentTimeMillis()

          idToRxBps.updateNetworkStats(slaveId, newRxBps)
          idToTxBps.updateNetworkStats(slaveId, newTxBps)
        } else {
          logWarning("Got heartbeat from unregistered slave " + slaveId)
        }
      }

      case Terminated(actor) => {
        // The disconnected actor could've been a slave or a client; remove accordingly.
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (actorToSlave.containsKey(actor))
          removeSlave(actorToSlave.get(actor))
        if (actorToClient.containsKey(actor))
          removeClient(actorToClient.get(actor))
      }

      case e: DisassociatedEvent => {
        // The disconnected actor could've been a slave or a client; remove accordingly.
        // Coflow termination is handled explicitly through UnregisterCoflow or when its client dies
        if (addressToSlave.containsKey(e.remoteAddress))
          removeSlave(addressToSlave.get(e.remoteAddress))
        if (addressToClient.containsKey(e.remoteAddress))
          removeClient(addressToClient.get(e.remoteAddress))
      }

      case RequestMasterState => {
        sender ! MasterState(
          ip,
          port,
          idToSlave.values.toSeq.toArray,
          idToCoflow.values.toSeq.toArray,
          completedCoflows.toArray,
          idToClient.values.toSeq.toArray)
      }

      case CheckForSlaveTimeOut => {
        timeOutDeadSlaves()
      }

      case RequestWebUIPort => {
        sender ! WebUIPortResponse(webUi.boundPort.getOrElse(-1))
      }

      case RequestBestRxMachines(howMany, bytes) => {
        sender ! BestRxMachines(idToRxBps.getTopN(
          howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case RequestBestTxMachines(howMany, bytes) => {
        sender ! BestTxMachines(idToTxBps.getTopN(
          howMany, bytes).toArray.map(x => idToSlave.get(x).host))
      }

      case AddFlows(flowDescs, coflowId, dataType) => {
        val currentSender = sender

        // coflowId will always be valid
        var coflow = idToCoflow.get(coflowId)
//        assert(coflow != null)
        if (coflow == null){
          this.synchronized{
            val emucoflow = idToEmuCoflow.get(coflowId)
//            assert(emucoflow != null)
            coflow = addCoflow(emucoflow)
          }
        }

        val st = now

        flowDescs.foreach { coflow.addFlow }
        logDebug("Added " + flowDescs.size + " flows to " + coflow + " in " + (now - st) +
          " milliseconds")

        // if the coflow is ready to launch and has not been launched, launch it.
        if(coflow.readyToLaunch && !coflow.launched.getAndSet(true)){
          launchTask(coflowId)
        }


        currentSender ! true
      }

      case AddFlow(flowDesc) => {
        val currentSender = sender

        // coflowId will always be valid
        val coflow = idToCoflow.get(flowDesc.coflowId)
        assert(coflow != null)

        val st = now
        coflow.addFlow(flowDesc)
        logDebug("Added flow to " + coflow + " in " + (now - st) + " milliseconds")

        currentSender ! true
      }

      case GetFlow(flowId, coflowId, clientId, slaveId, _) => {
        logTrace("Received GetFlow(" + flowId + ", " + coflowId + ", " + slaveId + ", " + sender +
          ")")

        val currentSender = sender
        Future { handleGetFlow(flowId, coflowId, clientId, slaveId, currentSender) }
      }

      case GetFlows(flowIds, coflowId, clientId, slaveId, _) => {
        logTrace("Received GetFlows(" + flowIds + ", " + coflowId + ", " + slaveId + ", " + sender +
          ")")

        val currentSender = sender
        Future { handleGetFlows(flowIds, coflowId, clientId, slaveId, currentSender) }
      }

      case FlowProgress(flowId, coflowId, bytesSinceLastUpdate, isCompleted) => {
        // coflowId will always be valid
        val coflow = idToCoflow.get(coflowId)
        assert(coflow != null)

        val st = now
        coflow.updateFlow(flowId, bytesSinceLastUpdate, isCompleted)
        if (coflow.numFlowsToComplete == 0){
          removeCoflow(coflow)
        }

        logTrace("Received FlowProgress for flow " + flowId + " of " + coflow + " in " +
          (now - st) + " milliseconds")
      }

      case DeleteFlow(flowId, coflowId) => {
        // TODO: Actually do something; e.g., remove destination?
        // self ! ScheduleRequest
        // sender ! true
      }

      case ScheduleRequest => {
        schedule()
      }

      case StartEmu => {
        prepareCoflows(traceInfo)
        val startTime = now() + 5000
        logInfo("The emulation will start in 5 seconds.")
        actorToClient.keys().foreach { _ ! StartEmuInTime(startTime)}
      }
    }

    def launchTask(coflowId: String): Unit ={
      val coflowReducerList = idToCoflowTrace(coflowId).reducerList
      coflowReducerList.foreach { nodeNum =>
        val clientId = emuClientId(nodeNum.toString)
        val clientActor = idToClient(clientId).actor
        clientActor ! LaunchTask(coflowId)
      }
    }

    def prepareCoflows(traceInfo: coflowemu.framework.TraceInfo): Unit ={
      traceInfo.traceArray.foreach { coflow =>
        idToCoflowTrace.put(coflow.coflowId, coflow)
        val flowNum = coflow.transferMap.size
        val coflowSizeInBytes = (coflow.transferMap.values.sum * 1048576).ceil.toInt  //MB to B
        val coflowDesc = new CoflowDescription(coflow.coflowId, CoflowType.SHUFFLE, flowNum, coflowSizeInBytes)
        idToEmuCoflow.put(coflowDesc.name,coflowDesc)
        logInfo("Emulation coflow : " + coflowDesc + " added.")
      }
    }

//    def addEmuCoflow(desc: CoflowDescription): CoflowInfo = {
//      val now = System.currentTimeMillis()
////      val date = new Date(now)
//      val date = new Date(now)
//      val coflow = new CoflowInfo(now, desc.name, desc, null, date, null)
//
//      idToEmuCoflow.put(coflow.id, coflow)
//
//      // Update its parent client
//      //client.addCoflow(coflow)
//
//      coflow
//    }

    def addCoflow(desc: CoflowDescription): CoflowInfo = {
      val now = System.currentTimeMillis()
      val date = new Date(now)
      //TODO: FIX start time. not 0L
      val coflow = new CoflowInfo(now, desc.name, desc, null, date, null)

      idToCoflow.put(coflow.id, coflow)

      // Update its parent client
      //client.addCoflow(coflow)

      coflow
    }

    override def addClient(clientName: String, host: String, commPort: Int, actor: ActorRef): ClientInfo = {
      val date = new Date(now)
      val client = new ClientInfo(now, emuClientId(clientName), host, commPort, date, actor)
      idToClient.put(client.id, client)
      actorToClient(actor) = client
      addressToClient(actor.path.address) = client
      client
    }

    def emuClientId(clientName: String): String = {
      "EMUCLIENT-%04d".format(clientName.toInt)
    }

    def newEmuCoflowId(coflowid: String): String = {
      "EMUCOFLOW-%06d".format(coflowid.toInt)
    }
  }

}

private[coflowemu] object EmulationMaster {
  private val systemName = "coflowemuMaster"
  private val actorName = "Master"
  private val coflowemuUrlRegex = "coflowemu://([^:]+):([0-9]+)".r
  //private val traceFile = "/coflowemu/trace.txt"

  def main(argStrings: Array[String]) {
    val args = new MasterArguments(argStrings)
    val masterObj = new EmulationMaster(systemName, actorName, args.ip, args.port, args.webUiPort, args.traceFile)
    val (actorSystem, _) = masterObj.start()
    actorSystem.awaitTermination()
  }

  /**
    * Returns an `akka.tcp://...` URL for the Master actor given a coflowemu Url `coflowemu://host:ip`.
    */
  def toAkkaUrl(coflowemuUrl: String): String = {
    coflowemuUrl match {
      case coflowemuUrlRegex(host, port) =>
        "akka.tcp://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new CoflowemuException("Invalid master URL: " + coflowemuUrl)
    }
  }

}

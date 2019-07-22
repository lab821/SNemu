package coflowemu.framework

import akka.actor.ActorRef

import coflowemu.framework.master.{CoflowInfo, ClientInfo, SlaveInfo}

private[coflowemu] sealed trait FrameworkMessage extends Serializable

// Slave to Master
private[coflowemu] case class RegisterSlave(
    id: String,
    host: String,
    port: Int,
    webUiPort: Int,
    commPort: Int,
    publicAddress: String)
  extends FrameworkMessage

private[coflowemu] case class Heartbeat(
    slaveId: String,
    rxBps: Double,
    txBps: Double)
  extends FrameworkMessage

// Master to Slave
private[coflowemu] case class RegisteredSlave(
    masterWebUiUrl: String) 
  extends FrameworkMessage

private[coflowemu] case class RegisterSlaveFailed(
    message: String) 
  extends FrameworkMessage

// Client to Master
private[coflowemu] case class RegisterClient(
    clientName: String, 
    host: String, 
    commPort: Int)
  extends FrameworkMessage

private[coflowemu] case class RegisterCoflow(
    clientId: String, 
    coflowDescription: CoflowDescription) 
  extends FrameworkMessage

private[coflowemu] case class UnregisterCoflow(
    coflowId: String) 
  extends FrameworkMessage

private[coflowemu] case class RequestBestRxMachines(
    howMany: Int, 
    adjustBytes: Long) 
  extends FrameworkMessage

private[coflowemu] case class RequestBestTxMachines(
    howMany: Int, 
    adjustBytes: Long) 
  extends FrameworkMessage

// Master/Client to Client/Slave
private[coflowemu] case class RegisteredClient(
    clientId: String, 
    slaveId: String,
    slaveUrl:String) 
  extends FrameworkMessage

private[coflowemu] case class CoflowKilled(
    message: String) 
  extends FrameworkMessage

// Master to Client
private[coflowemu] case class RegisterClientFailed(
    message: String) 
  extends FrameworkMessage

private[coflowemu] case class RegisteredCoflow(
    coflowId: String) 
  extends FrameworkMessage

private[coflowemu] case class RegisterCoflowFailed(
    message: String) 
  extends FrameworkMessage

private[coflowemu] case class UnregisteredCoflow(
    coflowId: String) 
  extends FrameworkMessage

private[coflowemu] case class RejectedCoflow(
    coflowId: String,
    message: String) 
  extends FrameworkMessage

private[coflowemu] case class BestRxMachines(
    bestRxMachines: Array[String]) 
  extends FrameworkMessage

private[coflowemu] case class BestTxMachines(
    bestTxMachines: Array[String]) 
  extends FrameworkMessage

private[coflowemu] case class UpdatedRates(
    newRates: Map[DataIdentifier, Double]) 
  extends FrameworkMessage

// Client/Slave to Slave/Master
private[coflowemu] case class AddFlow(
    flowDescription: FlowDescription) 
  extends FrameworkMessage

private[coflowemu] case class AddFlows(
    flowDescriptions: Array[FlowDescription], 
    coflowId: String, 
    dataType: DataType.DataType) 
  extends FrameworkMessage

private[coflowemu] case class GetFlow(
    flowId: String, 
    coflowId: String, 
    clientId: String,
    slaveId: String,
    flowDesc: FlowDescription = null) 
  extends FrameworkMessage

private[coflowemu] case class GetFlows(
    flowIds: Array[String], 
    coflowId: String, 
    clientId: String,
    slaveId: String,
    flowDescs: Array[FlowDescription] = null) 
  extends FrameworkMessage

private[coflowemu] case class FlowProgress(
    flowId: String, 
    coflowId: String, 
    bytesSinceLastUpdate: Long, 
    isCompleted: Boolean)
  extends FrameworkMessage

private[coflowemu] case class DeleteFlow(
    flowId: String, 
    coflowId: String) 
  extends FrameworkMessage

// Slave/Master to Client/Slave
private[coflowemu] case class GotFlowDesc(
    flowDesc: FlowDescription) 
  extends FrameworkMessage

private[coflowemu] case class GotFlowDescs(
    flowDescs: Array[FlowDescription]) 
  extends FrameworkMessage

// EmulationMaster to Client/Slave
private[coflowemu] case class StartEmuInTime(
    startTimeStamp: Long)
  extends FrameworkMessage

private[coflowemu] case class LaunchTask(coflowId: String)
  extends FrameworkMessage

// Internal message in Client/Slave
private[coflowemu] case object StopClient

private[coflowemu] case class GetRequest(
    flowDesc: FlowDescription, 
    targetHost: String = null,
    targetCommPort: Int = 0) {
    
  // Not extending FrameworkMessage because it is NOT going through akka serialization
  // override def toString: String = "GetRequest(" + flowDesc.id+ ":" + flowDesc.coflowId + ")"
} 

// Internal message in Master
private[coflowemu] case object ScheduleRequest

private[coflowemu] case object CheckForSlaveTimeOut

private[coflowemu] case object RequestWebUIPort

private[coflowemu] case class WebUIPortResponse(webUIBoundPort: Int)

// Internal message in EmulationMaster
private[coflowemu] case object StartEmu

// MasterWebUI To Master
private[coflowemu] case object RequestMasterState

// Master to MasterWebUI
private[coflowemu] case class MasterState(
    host: String, 
    port: Int, 
    slaves: Array[SlaveInfo],
    activeCoflows: Array[CoflowInfo], 
    completedCoflows: Array[CoflowInfo],
    activeClients: Array[ClientInfo]) {

  def uri = "coflowemu://" + host + ":" + port
}

//  SlaveWebUI to Slave
private[coflowemu] case object RequestSlaveState

// Slave to SlaveWebUI
private[coflowemu] case class SlaveState(
    host: String, 
    port: Int, 
    slaveId: String, 
    masterUrl: String, 
    rxBps: Double,
    txBps: Double,
    masterWebUiUrl: String)

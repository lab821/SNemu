package coflowemu.services

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.util.Date
import java.util.concurrent.{CountDownLatch, ScheduledThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger
import java.util.Timer
import java.util.TimerTask

import scala.collection.mutable.{ArrayBuffer, ListBuffer, HashMap}
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import coflowemu.util.AkkaUtils
import coflowemu.{Logging, Utils, CoflowemuException}
import coflowemu.framework.client._
import coflowemu.framework._


private[coflowemu] object EmulationSlave extends Logging {


  val asReducer = new HashMap[String, TraceCoflowInfo]()
  val asMapper = new HashMap[String, TraceCoflowInfo]()
  var maxTransfer = 0.0
  var nodeNum = 0
  var client: CoflowemuClient = null

  final val emulationStart = new CountDownLatch(1)
  var emuStartTime: Long = 0
  val reducerExecutor = Utils.newDaemonCachedThreadPool()

  class EmuListener extends EmulationListener with Logging {
    override def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    override def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }

    override def startEmulation(startTimeStamp: Long): Unit = {
      logInfo("Emulation will start at timestamp: " + startTimeStamp)
      emuStartTime = startTimeStamp
      emulationStart.countDown()
    }

    override def launchTask(coflowId: String): Unit = {
      logInfo("Prepare to launch task transfer of " + coflowId + " at node " + nodeNum)
      reducerExecutor.execute(new Runnable {
        override def run(): Unit = {
          val coflowTransferInfo = asReducer(coflowId)
          val flowIdArray = coflowTransferInfo.mapperList
            .filter(coflowTransferInfo.transferMap.containsKey(_, nodeNum))
            .map(node => (node, coflowTransferInfo.transferMap(node, nodeNum)))
//            .filter(_._2 != 0)
            .map(transfer => Utils.emulationFlowId(coflowTransferInfo.coflowNum, transfer._1, nodeNum))
            .toArray
          client.getFakeMultiple(flowIdArray, coflowId)
          logInfo("The transfer of coflow " + coflowId + " at node " + nodeNum + " completed")
        }
      })
    }
  }

  def exitGracefully(exitCode: Int) {
    System.exit(exitCode)
  }

  def now(): Long ={
    System.currentTimeMillis()
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("USAGE: EmulationSlave <masterUrl> <pathToTraceFile> <nodeNum>")
      System.exit(1)
    }

    val url = args(0)
    val pathToTraceFile = args(1)
    nodeNum = args(2).toInt

    val traceInfo = TraceInfo.parseTrace(pathToTraceFile)

    for (coflow <- traceInfo.traceArray) {
      if (coflow.mapperList.contains(nodeNum)) {
        asMapper.put(coflow.coflowId, coflow)
//        for (reducer <- coflow.reducerList) {
//          val transfer = coflow.transferMap((nodeNum, reducer))
//          if (transfer > maxTransfer) maxTransfer = transfer
//        }
      }
      if (coflow.reducerList.contains(nodeNum)) {
        asReducer.put(coflow.coflowId, coflow)
      }
    }

    val listener = new EmuListener
    client = new CoflowemuClient(nodeNum.toString, url, listener)
    client.start()

    try {
      emulationStart.await()
      val nowTime = System.currentTimeMillis()
      if (emuStartTime < nowTime) {
        logError("Emulation start time has passed.")
        System.exit(1)
      }
      val mapStatusUpdateTimer = new Timer()
      for (mapTransfer <- asMapper.values) {
        val coflowStartTime = emuStartTime + mapTransfer.startTimeMS
        val coflowStartDate = new Date(coflowStartTime)
        val transferDesc = mapTransfer.reducerList
          .filter(mapTransfer.transferMap.containsKey(nodeNum, _))
          .map(node => (node, mapTransfer.transferMap(nodeNum, node)))
//          .filter(_._2 != 0)
          .map(transfer => (Utils.emulationFlowId(mapTransfer.coflowNum, nodeNum, transfer._1), (1024 * 1024 * transfer._2.ceil).toLong, 1))
          .toArray
        logInfo("Coflow of id " + mapTransfer.coflowId + " will be registered at timestamp " + coflowStartTime)
        logInfo("    at date " + coflowStartDate)
        mapStatusUpdateTimer.schedule(new TimerTask {
          override def run(): Unit = {
//            val now = System.currentTimeMillis()
            val relativeTime = now() - coflowStartTime
            logInfo("Add emulation flows from coflow " + mapTransfer.coflowId + " at relative time " + relativeTime + "ms")
            client.putFakeMultiple(transferDesc, mapTransfer.coflowId)
          }
        }, coflowStartDate)
      }
    } catch {
      case e: Exception =>
        logError(e.toString)
        exitGracefully(1)
    }

    client.awaitTermination()
  }
}
package coflowemu.framework.master.ui

import akka.pattern.ask

import javax.servlet.http.HttpServletRequest

import net.liftweb.json.JsonAST.JValue

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.Node

import coflowemu.framework.FrameworkWebUI
import coflowemu.framework.{MasterState, RequestMasterState}
import coflowemu.framework.JsonProtocol
import coflowemu.framework.master.{CoflowInfo, SlaveInfo, ClientInfo}
import coflowemu.ui.UIUtils
import coflowemu.Utils

private[coflowemu] class IndexPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  implicit val timeout = parent.timeout

  def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30.seconds)
    JsonProtocol.writeMasterState(state)
  }

  /** Index view listing coflows and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30.seconds)

    val slaveHeaders = Seq("地址", "状态")
    val slaves = state.slaves.sortBy(_.id)
    val slaveTable = UIUtils.listingTable(slaveHeaders, slaveRow, slaves)

    val coflowHeaders = Seq("ID", "开始时间", "状态", "运行时间")
    val activeCoflows = state.activeCoflows.sortBy(_.startTime).reverse
    val activeCoflowsTable = UIUtils.listingTable(coflowHeaders, coflowRow, activeCoflows)
    val completedCoflows = state.completedCoflows.sortBy(_.endTime).reverse
    val completedCoflowsTable = UIUtils.listingTable(coflowHeaders, coflowRow, completedCoflows)

    val content =
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>驱动节点地址:</strong> {state.host}</li>
              <li><strong>计算节点数量:</strong> {state.slaves.size}</li>
              <li><strong>网络流组:</strong>
                {state.activeCoflows.size} 运行中,
                {state.completedCoflows.size} 已完成 </li>
            </ul>
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> 计算仿真节点 </h4>
            {slaveTable}
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> 运行中网络流组 </h4>
            {activeCoflowsTable}
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> 已完成网络流组 </h4>
            {completedCoflowsTable}
          </div>
        </div>;
    UIUtils.basicVarysPage(content, "网络流组仿真工具")
  }

  def slaveRow(slave: SlaveInfo): Seq[Node] = {
    <tr>
      <td>{slave.host}</td>
      <td>{slave.state}</td>
    </tr>
  }


  def coflowRow(coflow: CoflowInfo): Seq[Node] = {
    <tr>
      <td>{coflow.desc.name}</td>
      <td>{FrameworkWebUI.formatDate(coflow.submitDate)}</td>
      <td>{coflow.curState.toString}</td>
      <td>{FrameworkWebUI.formatDuration(coflow.duration)}</td>
    </tr>
  }
}

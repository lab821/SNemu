package coflowemu.framework.client

/**
  * Created by networking on 2018/11/12.
  */
abstract class EmulationListener extends ClientListener{

  def startEmulation(startTimeStamp: Long): Unit

  def launchTask(coflowId: String): Unit
}

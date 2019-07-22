package coflowemu.framework



import scala.collection.mutable
import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}

/**
  * Created by networking on 2018/8/21.
  */
private[coflowemu] class TraceCoflowInfo(val coflowNum: Long, val startTimeMS: Long) {

  val mapperList = new ListBuffer[Int]()
  val reducerList = new ListBuffer[Int]()
  val transferMap = new mutable.HashMap[(Int, Int), Double]()
  var mapperCount, reducerCount = 0
  val coflowId = "EMUCOFLOW-%06d".format(coflowNum.toInt)


  def addMapper(mapper: Int): Unit ={
    mapperList += mapper
  }

  def addReducer(reducer: Int): Unit ={
    reducerList += reducer
  }

  def addTransfer(mapperIndex: Int, reducerIndex: Int, transferMBCount: Double): Unit ={
    transferMap.put((mapperList(mapperIndex), reducerList(reducerIndex)), transferMBCount)
  }
}

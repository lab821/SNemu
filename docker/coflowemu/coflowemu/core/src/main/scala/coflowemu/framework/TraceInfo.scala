package coflowemu.framework

import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.io.IOException

private[coflowemu] class TraceInfo {
  var traceHostNum = 0
  var traceCoflowNum = 0
  val traceArray = new ListBuffer[TraceCoflowInfo]()
}

private[coflowemu] object TraceInfo{
  def parseTrace(traceFile: String): TraceInfo = {
    val traceInfo = new TraceInfo
    //val traceFileAdd = this.getClass.getResource("/") + traceFile (traceFileAdd.substring(5))
    try {
      val traceSource = Source.fromFile(traceFile).getLines() //  shen TM yao qu diao qian mian de "file:/"
      //logInfo("Found trace file at " + traceFileAdd)
      if (traceSource.hasNext) {
        val tracePair = traceSource.next().split("\\s+")
        traceInfo.traceHostNum = tracePair(0).toInt
        traceInfo.traceCoflowNum = tracePair(1).toInt
        //logInfo("Parsed Tracefile at " + traceFileAdd + " of " + traceHostNum + " hosts and " + traceCoflowNum + " coflows")
      }
      while (traceSource.hasNext) {
        val rawCoflow = traceSource.next().split("\\s+")
        val coflow = new TraceCoflowInfo(rawCoflow(0).toInt, rawCoflow(1).toInt)
        val numMapper = rawCoflow(2).toInt
        coflow.mapperCount = numMapper
        var pos = 3
        for (i <- 1 to numMapper) {
          coflow.addMapper(rawCoflow(pos).toInt)
          pos = pos + 1
        }
        val numReducer = rawCoflow(pos).toInt
        coflow.reducerCount = numReducer
        pos = pos + 1
        for (i <- 1 to numReducer) {
          coflow.addReducer(rawCoflow(pos).toInt)
          pos = pos + 1
        }
        for {x <- 1 to numReducer
             y <- 1 to numMapper} {
          if(rawCoflow(pos).toDouble != 0) {
            coflow.addTransfer(y - 1, x - 1, rawCoflow(pos).toDouble)
          }
          pos = pos + 1
        }
        traceInfo.traceArray += coflow
      }
      traceInfo
    } catch {
      case ex: IOException =>
        ex.printStackTrace()
        traceInfo
    }
  }
}


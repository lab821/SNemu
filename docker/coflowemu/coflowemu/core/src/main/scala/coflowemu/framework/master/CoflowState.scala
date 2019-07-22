package coflowemu.framework.master

private[coflowemu] object CoflowState extends Enumeration {
  
  type CoflowState = Value

  val WAITING, READY, RUNNING, FINISHED, FAILED, REJECTED = Value
}

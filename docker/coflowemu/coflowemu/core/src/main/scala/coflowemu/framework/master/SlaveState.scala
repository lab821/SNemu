package coflowemu.framework.master

private[coflowemu] object SlaveState extends Enumeration {
  type SlaveState = Value

  val ALIVE, DEAD, DECOMMISSIONED = Value
}

package com.ycf.flink.graph.context

/**
  * Created by yuchunfan on 2020/4/21.
  */
class TaskContext(name: String, startTime: Long, endTime: Long) {
  override def toString = s"$name: [$startTime TO $endTime] => COST: ${endTime - startTime}"

  def getName = name
  def getStartTime = startTime
  def getEndTime = endTime
  def cost = this.endTime - this.startTime
}

package com.ycf.flink.graph.context

import java.util.Properties

import scala.collection.mutable.ListBuffer

/**
  * Created by yuchunfan on 2020/4/21.
  */
class MsgContext(var msg: Any, params: Properties) {
  private val taskList: ListBuffer[TaskContext] = ListBuffer[TaskContext]()

  def setMsg(msg: Any): MsgContext = {
    this.msg = msg
    this
  }

  def getMsg = this.msg
  def getParams = this.params
  def getTaskContexts = this.taskList
  def getStartTime = this.taskList.head.getStartTime
  def getEndTime = this.taskList.last.getEndTime
  def getSumCost = getEndTime - getStartTime

  def addTaskContext(taskContext: TaskContext): Unit = {
    taskList += taskContext
  }

  def addTaskContext(taskContexts: List[TaskContext]): Unit = {
    taskList.append(taskContexts:_*)
  }

  override def toString = s"MsgContext:${params.toString} => $msg\nCosts: ${getSumCost}ms\n" + this.taskList.mkString("\n")
}

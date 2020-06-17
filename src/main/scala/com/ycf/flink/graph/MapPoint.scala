package com.ycf.flink.graph

import java.time.{LocalDateTime, ZoneOffset}

import com.ycf.flink.graph.context.{MsgContext, TaskContext}
import org.apache.flink.api.common.functions.RichMapFunction

trait MapPoint[IN, OUT] extends RichMapFunction[MsgContext, MsgContext] with Point {

  override def map(value: MsgContext): MsgContext = {
    val t: IN = value.getMsg.asInstanceOf[IN]

    val startTime = LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli
    val u: OUT = process(t)
    val endTime = LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli

    value.addTaskContext(new TaskContext(this.getClass.getName, startTime, endTime))

    value.setMsg(u)
  }

  def process(t: IN): OUT
}

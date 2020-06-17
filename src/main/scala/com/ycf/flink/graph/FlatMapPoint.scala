package com.ycf.flink.graph

import java.time.{LocalDateTime, ZoneOffset}

import com.ycf.flink.graph.context.{MsgContext, TaskContext}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

trait FlatMapPoint[IN, OUT] extends RichFlatMapFunction[MsgContext, MsgContext] with Point {

  override def flatMap(value: MsgContext, out: Collector[MsgContext]) = {
    val t: IN = value.getMsg.asInstanceOf[IN]

    val startTime = LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli
    val uSeq: Seq[OUT] = process(t)
    val endTime = LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli

    value.addTaskContext(new TaskContext(this.getClass.getName, startTime, endTime))

    uSeq.map(x => {
      val newMsgContext = new MsgContext(x, value.getParams)
      newMsgContext.addTaskContext(value.getTaskContexts.toList)
      out.collect(newMsgContext)
    })
  }

  def process(t: IN): Seq[OUT]
}

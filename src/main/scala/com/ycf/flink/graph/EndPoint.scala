package com.ycf.flink.graph

import com.ycf.flink.graph.context.MsgContext
import org.apache.flink.streaming.api.scala.DataStream

trait EndPoint extends Point{
  def process(dataStream: DataStream[MsgContext])
}

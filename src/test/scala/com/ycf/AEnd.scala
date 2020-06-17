package com.ycf

import com.ycf.flink.graph.EndPoint
import com.ycf.flink.graph.context.MsgContext
import org.apache.flink.streaming.api.scala.DataStream

class AEnd extends EndPoint{
  override def process(dataStream: DataStream[MsgContext]): Unit = {
    dataStream.print()
  }
}

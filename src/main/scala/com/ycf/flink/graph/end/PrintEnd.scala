package com.ycf.flink.graph.end

import com.ycf.flink.graph.EndPoint
import com.ycf.flink.graph.context.MsgContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

class PrintEnd extends EndPoint {

  override def process(dataStream: DataStream[MsgContext]): Unit = {
    dataStream.map(x => x.getMsg.toString).print()
  }
}

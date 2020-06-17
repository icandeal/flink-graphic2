package com.ycf.flink.graph

import com.ycf.flink.graph.context.MsgContext
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * Created by yuchunfan on 2020/3/23.
  */
trait TablePoint extends Point {

  /**
    * table API 操作
    *
    * @param dataStream 可以使用的输入流，如果是根节点，输入流为graph输入流（包装成MsgContext之后的）
    * @return 输出流，可以重新包装成MsgContext流，参与graph计算，也可以为null作为无输出点或结束点
    */
  def process(tenv: StreamTableEnvironment, dataStream: DataStream[MsgContext]): DataStream[MsgContext]
}

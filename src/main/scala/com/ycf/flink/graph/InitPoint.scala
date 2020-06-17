package com.ycf.flink.graph

import com.ycf.flink.graph.context.MsgContext
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
  * Created by yuchunfan on 2020/2/20.
  */
abstract class InitPoint extends RichMapFunction[MsgContext, MsgContext]{
  override def map(value: MsgContext): MsgContext = value

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    init()
  }

  /**
    * init common resources
    */
  def init(): Unit
}

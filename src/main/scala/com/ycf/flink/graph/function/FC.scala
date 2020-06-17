package com.ycf.flink.graph.function

/**
  * Created by yuchunfan on 2019/7/22.
  */
object FC {
  def keyFilter[T]: (Any, T) => Boolean = (x, y) => {
    val tuple = x.asInstanceOf[(T, String)]
    tuple._1.equals(y)
  }

  def notNull: Any => Boolean = _ != null
}

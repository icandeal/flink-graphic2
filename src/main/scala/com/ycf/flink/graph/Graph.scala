package com.ycf.flink.graph

import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties

import com.ycf.flink.graph.context.{MsgContext, TaskContext}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.json.JSONObject

import scala.collection.mutable.Map
/**
 * Created by yuchunfan on 2020/6/17.
 */

object Graph {
  def draw[T](dataStream: DataStream[T]): Graph[T] = {
    new Graph[T](dataStream)
  }
}

class Graph[T](dataStream: DataStream[T]) extends Serializable {

  private var tenv: StreamTableEnvironment = null

  private val pointMap = Map[String, Point]()
  private val edgeMap = Map[String, Map[String, AnyRef => Boolean]]()
  private var func: T => MsgContext = data => {
    val json = new JSONObject(data.toString)
    val topic = json.get("topic").toString
    val partition = json.get("partition").toString
    val offset = json.get("offset").toString

    val params = new Properties()
    params.setProperty("topic", topic)
    params.setProperty("partition", partition)
    params.setProperty("offset", offset)

    val msgContext = new MsgContext(data, params)
    val time = LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli
    val taskContext = new TaskContext(Graph.getClass.getName, time, time)
    msgContext.addTaskContext(taskContext)
    msgContext
  }
  private var initPoint: InitPoint = _

  /**
   * 给图添加点
   *
   * @param name  点的名称（名称唯一不可重复）
   * @param point 点实例
   */
  def addPoint(name: String, point: Point) = {
    pointMap(name) = point
  }

  /**
   * 给图添加边
   *
   * @param from   边起始点的名字
   * @param to     边终点的名字
   * @param filter 通过边的条件
   */
  def addEdge(from: String, to: String, filter: AnyRef => Boolean): Unit = {
    val filterMap = edgeMap.get(from)
    if (filterMap != None) {
      val map = filterMap.get
      map += (to -> filter)
    }
    else {
      edgeMap += (from -> Map(to -> filter))
    }
  }

  def enableTableEnv(tenv: StreamTableEnvironment) = {
    this.tenv = tenv
  }

  /**
   * 数据初始化方法，将进入的数据转成MsgContext进行传输
   *
   * @param func
   */
  def setMsgContextFunc(func: T => MsgContext): Unit = this.func = func

  def setInitPoint(initPoint: InitPoint) = this.initPoint = initPoint

  /**
   * 完成作图
   */
  def finish(): Unit = {
    val outSize = edgeMap.map(x => (x._1, x._2.size))
    val inSize = edgeMap.map(x => x._2.toList).reduce((a, b) => a ::: b).groupBy(x => x._1).map(x => (x._1, x._2.size))
    val rootPoint = pointMap.filter(x => inSize.getOrElse(x._1, 0) == 0).toList.sortBy(x => outSize.getOrElse(x._1, 0))


    rootPoint.foreach(x => {
      linkPoint[T](x._1, x._2, {
        if (initPoint != null)
          dataStream.map(func).map(initPoint)
        else
          dataStream.map(func)
      })
    })
  }

  /**
   * 用边将点连接起来
   *
   * @param name       起点
   * @param point      起点对象
   * @param dataStream 进入点的数据流
   * @tparam U
   */
  private def linkPoint[U](name: String, point: Point, dataStream: DataStream[MsgContext]): Unit = {
    point match {
      case value: MapPoint[U, _] =>
        if (!edgeMap.get(name).isDefined) throw new IllegalStateException("MapPoint 没有下游节点！！")
        val afDataStream = dataStream.map(value)
        val toMap = edgeMap.get(name).get
        toMap.foreach(x => linkTo(x, afDataStream))
      case value: FlatMapPoint[U, _] =>
        if (!edgeMap.get(name).isDefined) throw new IllegalStateException("FlatMapPoint 没有下游节点！！")
        val afDataStream = dataStream.flatMap(value)
        val toMap = edgeMap.get(name).get
        toMap.foreach(x => linkTo(x, afDataStream))
      case value: TablePoint =>
        val tp = value.asInstanceOf[TablePoint]
        val optionMap = edgeMap.get(name)
        val afDataStream = tp.process(tenv, dataStream)
        if (optionMap != None) {
          val toMap = optionMap.get
          toMap.foreach(x => linkTo(x, afDataStream))
        }
      case _ =>
        val end = point.asInstanceOf[EndPoint]
        end.process(dataStream)
    }
  }

  private def linkTo(x: (String, AnyRef => Boolean), afDataStream: DataStream[MsgContext]) = {
    val toName = x._1
    val func = x._2
    val toPoint = pointMap.get(toName).get

    var toDataStream = afDataStream
    if (func != null) {
      toDataStream = afDataStream.filter(x => func(x.getMsg.asInstanceOf[AnyRef]))
    }
    linkPoint(toName, toPoint, toDataStream)
  }
}

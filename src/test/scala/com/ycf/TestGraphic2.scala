package com.ycf

import java.util.Properties

import com.ycf.flink.graph.{Graph, InitPoint}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumerBase}
import org.junit.{Before, Test}
import org.apache.flink.api.scala._

import scala.collection.convert.wrapAsJava._

class TestGraphic2 {
  val properties = new Properties()
  var consumer011: FlinkKafkaConsumerBase[String] = null
  var senv: StreamExecutionEnvironment = null
  var dataStream: DataStream[String] = null

  @Before
  def before(): Unit = {
    properties.setProperty("bootstrap.servers", "localhost:9092")
    //    properties.setProperty("bootstrap.servers", "t45:9092")
    properties.setProperty("group.id", "test1")
    properties.setProperty("auto.offset.reset", "latest")

    consumer011 = new FlinkKafkaConsumer011[String](
      List("test1"),
      //      List("t45.test.user"),
      new SimpleStringSchema(),
      properties
    ).setStartFromEarliest()
    senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(500)
    dataStream = senv.addSource(consumer011)
  }


  @Test
  def testGraphic(): Unit ={
    val graph = Graph.draw(dataStream)

//    val hbaseConf = new Properties()
//    hbaseConf.setProperty("hbase.zookeeper.quorum", "test-hadoop1,test-hadoop2,test-hadoop3")
//    hbaseConf.setProperty("hbase.zookeeper.property.clientPort", "2181")


    graph.setInitPoint(new InitPoint {
      /**
        * init common resources
        */
      override def init(): Unit = {
        // do some init
//        HbaseUtil.init(hbaseConf)
      }
    })

    graph.addPoint("1", new AMap)
    graph.addPoint("2", new AFlatMap)
    graph.addPoint("3", new AEnd)

    graph.addEdge("1", "2", null)
    graph.addEdge("2", "3", null)

    graph.finish()

    senv.execute()
  }
}

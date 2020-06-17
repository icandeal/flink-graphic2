package com.ycf

import com.ycf.flink.graph.FlatMapPoint

class AFlatMap extends FlatMapPoint[String, String]{
  override def process(t: String): Seq[String] = {
    t.split(",").toSeq.map(x => {
//      HbaseUtil.insert("test_hbase_error", Random.nextString(7), "test", "USER_NAME", x)
//      HbaseUtil.getColumnValue("USER_INFO_ORACLE", "100001205", "USER_INFO", "USER_NAME") + x
      x
    })
  }
}

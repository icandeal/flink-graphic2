package com.ycf

import com.ycf.flink.graph.MapPoint

class AMap extends MapPoint[String, String]{

  override def process(t: String): String = {
    t
  }
}

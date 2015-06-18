package edu.clarkson.cs.itop.app.trafficana

import java.math.BigDecimal
import java.util.ArrayList

class TrafficTuple extends Storable {

  var averageLatency = BigDecimal.ZERO;

  var numOfPath = 0;

  def addLatency(input: BigDecimal) = {
    averageLatency = averageLatency
      .multiply(new BigDecimal(numOfPath))
      .add(input)
      .divide(new BigDecimal(numOfPath + 1));
    numOfPath += 1;
  }

  def serialize(): String = {
//    return Json.serialize(this);
    return "";
  }

  def deserialize(input:String): Unit = {
//    var jsonObject = Json.deserialize(input);
//    this.averageLatency = jsonObject.getBigDecimal("averageLatency");
//    this.numOfPath = jsonObject.getInteger("numOfPath");
  }
}

class TracerouteData {

  var sections: java.util.List[Section] = new ArrayList[Section]();
}

class Section {

  var length = 0;

  var fromIP: String = "";

  var toIP: String = "";

  var latency = java.math.BigDecimal.ZERO;
}


package edu.clarkson.cs.itop.core.conhash.message

import java.util.HashMap

class CopyResponse {
  var toLocation: BigDecimal = null;
  var fromLocation: BigDecimal = null;
  var fromNode = "";

  var content: java.util.Map[String, String] = new HashMap[String, String];

  def this(fn: String, fl: BigDecimal, tl: BigDecimal) = {
    this();
    fromNode = fn;
    fromLocation = fl;
    toLocation = tl;
  }
}
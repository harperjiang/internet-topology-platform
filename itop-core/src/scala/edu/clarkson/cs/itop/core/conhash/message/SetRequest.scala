package edu.clarkson.cs.itop.core.conhash.message

class SetRequest {
  var nodeid = "";
  var location: BigDecimal = null;
  var key = "";
  var value = "";

  def this(nid: String, loc: BigDecimal, k: String, v: String) = {
    this();
    nodeid = nid;
    location = loc;
    key = k;
    value = v;
  }
}
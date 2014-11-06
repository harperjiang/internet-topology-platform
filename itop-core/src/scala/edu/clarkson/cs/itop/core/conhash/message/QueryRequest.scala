package edu.clarkson.cs.itop.core.conhash.message

class QueryRequest {
  var nodeid = "";
  var location: BigDecimal = null;
  var key = "";
  var sessionKey = "";

  def this(nid: String, loc: BigDecimal, k: String, sk: String) = {
    this();
    nodeid = nid;
    location = loc;
    key = k;
    sessionKey = sk;
  }
}
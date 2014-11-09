package edu.clarkson.cs.itop.core.conhash.message

class QueryResponse {
  var nodeid = ""
  var fromloc: BigDecimal = null;
  var result = "";
  var sessionKey = "";

  def this(nid: String, fl: BigDecimal, r: String, sk: String) = {
    this();
    nodeid = nid;
    fromloc = fl;
    result = r;
    sessionKey = sk;
  }
}
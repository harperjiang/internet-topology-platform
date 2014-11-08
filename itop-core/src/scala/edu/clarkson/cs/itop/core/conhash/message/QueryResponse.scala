package edu.clarkson.cs.itop.core.conhash.message

class QueryResponse {
  var nodeid = ""
  var result = "";
  var sessionKey = "";

  def this(nid: String, r: String, sk: String) = {
    this();
    nodeid = nid;
    result = r;
    sessionKey = sk;
  }
}
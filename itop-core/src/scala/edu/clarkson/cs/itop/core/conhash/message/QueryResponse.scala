package edu.clarkson.cs.itop.core.conhash.message

class QueryResponse {
  var result = "";
  var sessionKey = "";

  def this(r: String, sk: String) = {
    this();
    result = r;
    sessionKey = sk;
  }
}
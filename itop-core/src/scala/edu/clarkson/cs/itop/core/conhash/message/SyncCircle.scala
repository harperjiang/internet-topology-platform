package edu.clarkson.cs.itop.core.conhash.message

class SyncCircleRequest {
  var id = "";

  def this(nid: String) = {
    this();
    this.id = nid;
  }
}

class SyncCircleResponse {
  var circle: java.util.Set[String] = null;

  def this(data: java.util.Set[String]) = {
    this();
    circle = data;
  }
}
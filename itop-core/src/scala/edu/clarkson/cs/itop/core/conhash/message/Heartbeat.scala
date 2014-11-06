package edu.clarkson.cs.itop.core.conhash.message

class Heartbeat {

  var storeId = "";
  var time = 0l;

  def this(sid: String, t: Long) = {
    this();
    storeId = sid;
    time = t;
  }
}
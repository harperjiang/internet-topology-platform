package edu.clarkson.cs.itop.core.conhash.message

class StoreAddMessage {
  var storeId = "";

  def this(sid: String) = {
    this();
    storeId = sid;
  }
}

class StoreRemoveMessage {
  var storeId = "";

  def this(sid: String) = {
    this();
    storeId = sid;
  }
}
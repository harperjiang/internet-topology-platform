package edu.clarkson.cs.itop.core.conhash.message

trait StoreMessage {
  var storeId = "";
}

class StoreAddMessage extends StoreMessage {

  def this(sid: String) = {
    this();
    storeId = sid;
  }
}

class StoreRemoveMessage extends StoreMessage {

  def this(sid: String) = {
    this();
    storeId = sid;
  }
}
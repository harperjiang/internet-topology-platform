package edu.clarkson.cs.itop.core.external

trait KeyValueStore {
  
  def get(key: String): String;
  
  def set(key: String, value: String): Unit;
  
}
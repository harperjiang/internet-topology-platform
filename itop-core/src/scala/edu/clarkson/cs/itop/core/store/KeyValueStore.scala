package edu.clarkson.cs.itop.core.store

trait KeyValueStore {
  
  def get(key: String): String;
  
  def set(key: String, value: String): Unit;
  
}
package edu.clarkson.cs.itop.core.store

trait KeyValueStore {

  def get(key: String): String;

  def set(key: String, value: String): Unit;

  def getObject[T](key: String, clazz: Class[T]): T = {
    throw new UnsupportedOperationException();
  }

  def setObject[T <: AnyRef](key: String, value: T): Unit = {
    throw new UnsupportedOperationException();
  }
}
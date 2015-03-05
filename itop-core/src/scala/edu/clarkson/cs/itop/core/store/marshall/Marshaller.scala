package edu.clarkson.cs.itop.core.store.marshall

trait Marshaller {

  def marshall[T](obj: T): String;

  def unmarshall[T](data: String, clazz: Class[T]): T;
}
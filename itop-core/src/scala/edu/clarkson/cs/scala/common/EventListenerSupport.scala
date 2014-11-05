package edu.clarkson.cs.scala.common

import java.util.EventListener

import scala.collection.mutable.ArrayBuffer

trait EventListenerSupport[T <: EventListener] {

  /**
   * Support to scheduler listeners
   */
  protected var listeners = new ArrayBuffer[T]();

  def addListener(l: T) = {
    listeners += l;
  }

  def removeListener(l: T) = {
    listeners -= l;
  }
}
package edu.clarkson.cs.itop.core.model

trait Routing {

  def route(nodeId: Int): Iterable[Int];
}
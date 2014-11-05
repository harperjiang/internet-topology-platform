package edu.clarkson.cs.itop.model

trait Routing {

  def route(nodeId: Int): Iterable[Int];
}
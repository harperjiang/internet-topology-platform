package edu.clarkson.cs.itop.core.conhash

trait HashCircle {

  def insert(locations: Iterable[Double], ref: Int): Unit;

  def find(location: Double, size: Int): Iterable[Int];
}
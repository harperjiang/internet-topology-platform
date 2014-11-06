package edu.clarkson.cs.itop.core.conhash

trait Distribution {

  def keyHash: (String) => Double;
  
  def idDist: (Int) => Iterable[Double];
}
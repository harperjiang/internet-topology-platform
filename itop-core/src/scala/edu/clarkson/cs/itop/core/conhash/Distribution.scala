package edu.clarkson.cs.itop.core.conhash

import scala.collection.mutable.ArrayBuffer

trait Distribution {

  def keyHash: (String) => BigDecimal;

  def idDist: (String) => Iterable[BigDecimal];
}

class DefaultDistribution extends Distribution {

  private val MAX = BigDecimal(Integer.MAX_VALUE);

  private val span = 4;

  def keyHash: (String) => BigDecimal = {
    return m => BigDecimal(m.toString().hashCode())
  }

  def idDist: (String) => Iterable[BigDecimal] = {
    return (id) => {
      var buffer = new ArrayBuffer[BigDecimal]();
      var base = BigDecimal(id.hashCode());
      buffer += base;

      for (i <- 2 to span)
        buffer += base.pow(i) % MAX;

      buffer;
    };
  }
}
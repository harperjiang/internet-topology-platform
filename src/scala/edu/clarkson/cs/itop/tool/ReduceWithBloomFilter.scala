package edu.clarkson.cs.itop.tool

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels

object ReduceWithBloomFilter extends App {

  // If a link had been merged once, do not merge it to other clusters.
  // We determine this by a bloom filter

  var bloomFilter =
    BloomFilter.create[Integer](Funnels.integerFunnel(), 57000000, 0.001);

  
}
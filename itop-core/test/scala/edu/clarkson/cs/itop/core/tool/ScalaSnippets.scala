package edu.clarkson.cs.itop.core.tool

import com.google.gson.GsonBuilder

object ScalaSnippets extends App {

  var a = Int.MaxValue - 123;
  var b = BigDecimal(a).pow(2) % Int.MaxValue;
  println(a);
  println(b);
}
package edu.clarkson.cs.itop.core.conhash.message

import java.util.HashMap

class CopyResponse {
  var toLocation: BigDecimal = null;
  var fromLocation: BigDecimal = null;
  var fromNode = "";

  var content: java.util.Map[String, String] = null;
}
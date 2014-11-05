package edu.clarkson.cs.itop.model

class NodeLink(n: Int, nip: String, l: Int) {
  var node = n
  var ip = nip
  var link = l

  def this(nid: String, nip: String, l: String) = {
    this(Integer.parseInt(nid.substring(1)), nip, Integer.parseInt(l.substring(1)))
  }

}
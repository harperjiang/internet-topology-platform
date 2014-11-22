package edu.clarkson.cs.itop.tool

object Config {

  def dir = "/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/test/"

  def file(name: String) = "%s%s".format(dir, name)
}
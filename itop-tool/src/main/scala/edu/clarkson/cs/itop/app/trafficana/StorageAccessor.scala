package edu.clarkson.cs.itop.app.trafficana

import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.model.Link

trait StorageAccessor {

  def store(node: Node, key: String, value: Storable);

  def store(link: Link, key: String, value: Storable);

  def retrieve(node: Node, key: String): Storable;

  def retrieve(link: Link, key: String): Storable;

}

trait Storable {

  def serialize(): String;

  def deserialize(input: String): Unit;
}
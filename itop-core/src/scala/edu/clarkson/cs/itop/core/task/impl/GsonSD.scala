package edu.clarkson.cs.itop.core.task.impl

import com.google.gson.JsonDeserializer
import com.google.gson.JsonSerializer
import com.google.gson.JsonSerializationContext
import java.lang.reflect.Type
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonElement
import edu.clarkson.cs.itop.core.model.Partition
import edu.clarkson.cs.itop.core.model.Index
import edu.clarkson.cs.httpjson.json.BeanSerializer
import edu.clarkson.cs.httpjson.json.BeanDeserializer
import com.google.gson.JsonObject
import com.google.gson.JsonArray
import scala.collection.JavaConversions._

/**
 * This file contains json serializer and deserializer that is used by system-provided workers such as DFS workers
 */

class IndexSerializer extends BeanSerializer[Index];
class IndexDeserializer extends BeanDeserializer[Index];

class PathNodeSD extends JsonSerializer[PathNode] with JsonDeserializer[PathNode] {

  var partition: Partition = null;

  override def serialize(src: PathNode, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    var elem = new JsonObject;

    elem.addProperty("nodeId", src.node.id);
    elem.addProperty("linkId", src.link.id);
    elem.add("nodeIndex", context.serialize(src.nodeIndex));
    elem.add("linkIndex", context.serialize(src.linkIndex));

    return elem;
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): PathNode = {
    var jsobj = json.asInstanceOf[JsonObject];

    var pn = new PathNode();

    var nodeId = jsobj.get("nodeId").getAsInt;
    pn.node = partition.nodeMap.get(nodeId).get
    var linkId = jsobj.get("linkId").getAsInt;
    pn.link = partition.linkMap.get(linkId).get

    pn.nodeIndex = context.deserialize(jsobj.get("nodeIndex"), classOf[Index]);
    pn.linkIndex = context.deserialize(jsobj.get("linkIndex"), classOf[Index]);
    return pn;
  }
}

class PathSD extends JsonSerializer[Path] with JsonDeserializer[Path] {

  override def serialize(src: Path, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    var array = new JsonArray();
    for (p <- src.nodes) {
      array.add(context.serialize(p))
    }
    return array;
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Path = {
    var path = new Path;

    var array = json.getAsJsonArray;
    for (a <- array) {
      path.nodes.add(context.deserialize(a, classOf[PathNode]));
    }
    return path;
  }
}

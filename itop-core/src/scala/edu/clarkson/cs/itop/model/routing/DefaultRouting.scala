package edu.clarkson.cs.itop.model.routing

import java.util.concurrent.ConcurrentHashMap
import scala.io.Source
import org.springframework.beans.factory.InitializingBean
import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import edu.clarkson.cs.itop.model.Routing


class DefaultRouting extends Routing with InitializingBean {

  var routingSize = 0;
  var routingFile = "";

  private var bloomFilter: BloomFilter[Integer] = null;
  private val hashIndex = new ConcurrentHashMap[Int, List[Int]];

  def afterPropertiesSet() = {
    bloomFilter = BloomFilter.create[Integer](
      Funnels.integerFunnel(), routingSize, 0.001);
    Source.fromFile(routingFile).getLines().foreach(line => {
      var split = line.split("\\s");
      var nodeId = split(0).toInt;
      bloomFilter.put(nodeId);
      hashIndex.put(nodeId, split.slice(1, split.length).map(a => a.toInt).toList);
    });
  }

  def route(nodeId: Int): Iterable[Int] = {
    if (bloomFilter.mightContain(nodeId)) {
      if (hashIndex.containsKey(nodeId)) {
        return hashIndex.get(nodeId);
      }
    }
    return Iterable.empty[Int];
  }
}
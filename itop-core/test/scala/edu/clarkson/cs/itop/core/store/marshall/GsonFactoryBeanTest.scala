package edu.clarkson.cs.itop.core.store.marshall

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import com.google.gson.Gson
import edu.clarkson.cs.itop.core.model.Index
import edu.clarkson.cs.itop.core.model.Link
import edu.clarkson.cs.itop.core.model.Node
import edu.clarkson.cs.itop.core.task.impl.Path
import edu.clarkson.cs.itop.core.task.impl.PathNode
import javax.annotation.Resource
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(classOf[SpringJUnit4ClassRunner])
@ContextConfiguration(locations = Array("classpath:app-context-worker.xml"))
class GsonFactoryBeanTest {

  @Resource(name = "gson")
  var gson: Gson = null;

  @Test
  def testIndex: Unit = {
    var index = new Index();
    index.onNamedItems = true;
    index.nameKey = "aka";
    index.anonymousIndex = 5;
    assertEquals("{\"anonymousIndex\":5,\"nameKey\":\"aka\",\"onNamedItems\":true}", gson.toJson(index).toString());

    var read = gson.fromJson("{\"anonymousIndex\":3,\"nameKey\":\"bdf\",\"onNamedItems\":false}", classOf[Index]);
    assertEquals("bdf", read.nameKey);
    assertEquals(3, read.anonymousIndex);
    assertEquals(false, read.onNamedItems);
  }

  @Test
  def testPathNode: Unit = {
    var nodeIndex = new Index();
    nodeIndex.nameKey = "323";
    nodeIndex.onNamedItems = true;
    nodeIndex.anonymousIndex = 3;

    var linkIndex = new Index();
    linkIndex.nameKey = "422";
    linkIndex.onNamedItems = false;
    linkIndex.anonymousIndex = 5;

    var pn = new PathNode(new Node(1), new Link(1), nodeIndex, linkIndex);

    var data = "{\"nodeId\":1,\"linkId\":1,\"nodeIndex\":{\"anonymousIndex\":3,\"nameKey\":\"323\",\"onNamedItems\":true},\"linkIndex\":{\"anonymousIndex\":5,\"nameKey\":\"422\",\"onNamedItems\":false}}";
    assertEquals(data, gson.toJson(pn));

    var read = gson.fromJson(data, classOf[PathNode]);
    assertEquals(1, read.node.id);
    assertEquals(1, read.link.id);
    assertEquals("323", read.nodeIndex.nameKey);
    assertEquals(true, read.nodeIndex.onNamedItems);
    assertEquals(3, read.nodeIndex.anonymousIndex)
    assertEquals("422", read.linkIndex.nameKey);
    assertEquals(false, read.linkIndex.onNamedItems);
    assertEquals(5, read.linkIndex.anonymousIndex);
  }

  @Test
  def testPath: Unit = {
    var nodeIndex = new Index();
    nodeIndex.nameKey = "323";
    nodeIndex.onNamedItems = true;
    nodeIndex.anonymousIndex = 3;

    var linkIndex = new Index();
    linkIndex.nameKey = "422";
    linkIndex.onNamedItems = false;
    linkIndex.anonymousIndex = 5;

    var pn = new PathNode(new Node(1), new Link(1), nodeIndex, linkIndex);

    var nodeIndex2 = new Index(nodeIndex);
    var linkIndex2 = new Index(linkIndex);
    nodeIndex2.anonymousIndex = 4;
    nodeIndex2.nameKey = "akaaf";
    linkIndex2.nameKey = "afde42";
    linkIndex2.onNamedItems = true;
    linkIndex2.anonymousIndex = 1;

    var pn2 = new PathNode(new Node(2), new Link(3), nodeIndex2, linkIndex2);

    var path = new Path();
    path.push(pn);
    path.push(pn2);

    var data = "[{\"nodeId\":1,\"linkId\":1,\"nodeIndex\":{\"anonymousIndex\":3,\"nameKey\":\"323\",\"onNamedItems\":true},\"linkIndex\":{\"anonymousIndex\":5,\"nameKey\":\"422\",\"onNamedItems\":false}},{\"nodeId\":2,\"linkId\":3,\"nodeIndex\":{\"anonymousIndex\":4,\"nameKey\":\"akaaf\",\"onNamedItems\":true},\"linkIndex\":{\"anonymousIndex\":1,\"nameKey\":\"afde42\",\"onNamedItems\":true}}]";

    assertEquals(data, gson.toJson(path));

    var read = gson.fromJson(data, classOf[Path]);

    assertEquals(2, read.length);
    var readpn1 = read.pop;
    var readpn2 = read.pop;

    assertEquals(2, readpn1.node.id);
    assertEquals(3, readpn1.link.id);
    assertEquals("akaaf", readpn1.nodeIndex.nameKey);
    assertEquals(true, readpn1.nodeIndex.onNamedItems);
    assertEquals(4, readpn1.nodeIndex.anonymousIndex)
    assertEquals("afde42", readpn1.linkIndex.nameKey);
    assertEquals(true, readpn1.linkIndex.onNamedItems);
    assertEquals(1, readpn1.linkIndex.anonymousIndex);

    assertEquals(1, readpn2.node.id);
    assertEquals(1, readpn2.link.id);
    assertEquals("323", readpn2.nodeIndex.nameKey);
    assertEquals(true, readpn2.nodeIndex.onNamedItems);
    assertEquals(3, readpn2.nodeIndex.anonymousIndex)
    assertEquals("422", readpn2.linkIndex.nameKey);
    assertEquals(false, readpn2.linkIndex.onNamedItems);
    assertEquals(5, readpn2.linkIndex.anonymousIndex);
  }

}
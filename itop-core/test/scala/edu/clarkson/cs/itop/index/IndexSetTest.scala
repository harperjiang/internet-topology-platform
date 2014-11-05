package edu.clarkson.cs.itop.index;

import java.io.FileInputStream
import java.io.ObjectInputStream
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import edu.clarkson.cs.itop.model.Node
import edu.clarkson.cs.itop.parser.Parser


class IndexSetTest {

  @Test
  def testBuild1 = {
    var indexSet = new IndexSet("testdata/index", 6);
    var parser = new Parser();
    indexSet.fileLevel = 5;
    indexSet build ("testdata/nodes_1",
      line => { !line.startsWith("#") },
      input => { var nl = parser.parse(input).asInstanceOf[Node]; nl.id })

    var ois = new ObjectInputStream(new FileInputStream("testdata/index/root"));
    var head = ois.readObject().asInstanceOf[IndexNode];
    ois.close

    assertEquals(10, head.max)
    assertEquals(1, head.min)

    assertEquals(2, head.depth)

    assertTrue(head.datas(0).isInstanceOf[OffsetLeaf]);
    assertTrue(head.datas(1).isInstanceOf[OffsetLeaf]);

    var leaf1 = head.datas(0).asInstanceOf[OffsetLeaf];
    var leaf2 = head.datas(1).asInstanceOf[OffsetLeaf];
    assertEquals(6, leaf1.offsets.size);
    assertEquals(4, leaf2.offsets.size);

    assertEquals(0x2e, leaf1.offsets(0)._2);
    assertEquals(69, leaf1.offsets(0)._3);

    assertEquals(69, leaf1.offsets(1)._2);
    assertEquals(92, leaf1.offsets(1)._3);

    assertEquals(161, leaf1.offsets(5)._2);
    assertEquals(184, leaf1.offsets(5)._3);

    assertEquals(184, leaf2.offsets(0)._2);
    assertEquals(208, leaf2.offsets(0)._3);

  }

//  @Test
  def testBuild2 = {
    var indexSet = new IndexSet("testdata/index", 3);
    var parser = new Parser();
    indexSet.fileLevel = 5;
    indexSet build ("testdata/nodes_2",
      line => { !line.startsWith("#") },
      input => { var nl = parser.parse(input).asInstanceOf[Node]; nl.id })

    var ois = new ObjectInputStream(new FileInputStream("testdata/index/root"));
    var head = ois.readObject().asInstanceOf[IndexNode];
    ois.close

    assertEquals(54875087, head.max)
    assertEquals(54875058, head.min)

    assertEquals(4, head.depth)

    assertEquals(1, head.datas(1).datas.size);
    assertEquals(1, head.datas(1).datas(0).datas.size);
    assertTrue(head.datas(1).datas(0).datas(0).isInstanceOf[OffsetLeaf]);

    var leaf = head.datas(0).datas(2).datas(2).asInstanceOf[OffsetLeaf];
    assertEquals(0x35d, leaf.offsets(2)._2);
    assertEquals(0x37f, leaf.offsets(2)._3);

    leaf = head.datas(1).datas(0).datas(0).asInstanceOf[OffsetLeaf];
    assertEquals(0x37f, leaf.offsets(0)._2);
    assertEquals(0x3a1, leaf.offsets(0)._3);

    assertEquals(0x3a1, leaf.offsets(1)._2);
    assertEquals(0x3c3, leaf.offsets(1)._3);

    assertEquals(0x3c3, leaf.offsets(2)._2);
    assertEquals(-1, leaf.offsets(2)._3);

  }

}

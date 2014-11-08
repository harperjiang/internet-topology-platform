package edu.clarkson.cs.itop.core.conhash

import org.junit.Test
import org.junit.Assert._
import java.security.MessageDigest

class DefaultHashFunctionTest {

  @Test
  def testKeyHash = {
    assertEquals(BigDecimal(92597939),
      new DefaultHashFunction().keyHash("abasf"))
  }

  @Test
  def testIdDist = {
    var iddist = new DefaultHashFunction().idDist("node1");
    assertEquals(4, iddist.size);
    assertTrue(iddist.exists(_==BigDecimal(104993391)))
    assertTrue(iddist.exists(_==BigDecimal(920526838)))
    assertTrue(iddist.exists(_==BigDecimal(1823073176)))
    assertTrue(iddist.exists(_==BigDecimal(1378512082)))
  }
}
package edu.clarkson.cs.itop.parser;

import java.io.StringReader

import java_cup.runtime.Symbol

import org.junit.Assert._
import org.junit.Test

import edu.clarkson.cs.itop.parser.Lexer;

class NodeLexerTest {

  @Test
  def testNextTokenOnNode() = {
    var lexer = new Lexer(new StringReader("node N13: 123.323.232.433 123.313.144.131"));

    var tokens = List(3, 7, 2, 6, 6, 0);
    tokens.foreach {
      i =>
        {
          var token = lexer.next_token();
          assertEquals(i, token.sym);
        }
    };

  }

  @Test
  def testNextTokenOnLink() = {
    var lexer = new Lexer(new StringReader("link L13: N123:123.323.232.433 N134:123.313.144.131 N22 N71"));
    var tokens = List(4, 8, 2, 7, 2, 6, 7, 2, 6, 7, 7, 0);
    tokens.foreach {
      i =>
        {
          var token = lexer.next_token();
          assertEquals(i, token.sym);
        }
    };
  }

}

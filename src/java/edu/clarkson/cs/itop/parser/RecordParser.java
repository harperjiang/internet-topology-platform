
//----------------------------------------------------
// The following code was generated by CUP v0.11a beta 20060608
// Fri Oct 10 14:40:28 EDT 2014
//----------------------------------------------------

package edu.clarkson.cs.itop.parser;

import java_cup.runtime.*;

import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;
import edu.clarkson.cs.itop.model.*;

/** CUP v0.11a beta 20060608 generated parser.
  * @version Fri Oct 10 14:40:28 EDT 2014
  */
public class RecordParser extends java_cup.runtime.lr_parser {

  /** Default constructor. */
  public RecordParser() {super();}

  /** Constructor which sets the default scanner. */
  public RecordParser(java_cup.runtime.Scanner s) {super(s);}

  /** Constructor which sets the default scanner. */
  public RecordParser(java_cup.runtime.Scanner s, java_cup.runtime.SymbolFactory sf) {super(s,sf);}

  /** Production table. */
  protected static final short _production_table[][] = 
    unpackFromStrings(new String[] {
    "\000\016\000\002\002\004\000\002\002\003\000\002\002" +
    "\003\000\002\002\003\000\002\004\006\000\002\003\006" +
    "\000\002\005\006\000\002\005\010\000\002\006\003\000" +
    "\002\006\004\000\002\007\003\000\002\007\004\000\002" +
    "\010\005\000\002\010\003" });

  /** Access to production table. */
  public short[][] production_table() {return _production_table;}

  /** Parse-action table. */
  protected static final short[][] _action_table = 
    unpackFromStrings(new String[] {
    "\000\034\000\010\005\004\006\005\007\007\001\002\000" +
    "\004\011\032\001\002\000\004\012\022\001\002\000\004" +
    "\002\021\001\002\000\004\011\013\001\002\000\004\002" +
    "\ufffe\001\002\000\004\002\000\001\002\000\004\002\uffff" +
    "\001\002\000\004\004\014\001\002\000\006\010\016\012" +
    "\015\001\002\000\004\002\ufffb\001\002\000\004\004\017" +
    "\001\002\000\004\012\020\001\002\000\004\002\ufffa\001" +
    "\002\000\004\002\001\001\002\000\004\004\023\001\002" +
    "\000\004\011\025\001\002\000\006\002\ufff7\011\ufff7\001" +
    "\002\000\010\002\ufff4\004\030\011\ufff4\001\002\000\006" +
    "\002\ufffc\011\025\001\002\000\006\002\ufff6\011\ufff6\001" +
    "\002\000\004\010\031\001\002\000\006\002\ufff5\011\ufff5" +
    "\001\002\000\004\004\033\001\002\000\004\010\035\001" +
    "\002\000\006\002\ufffd\010\036\001\002\000\006\002\ufff9" +
    "\010\ufff9\001\002\000\006\002\ufff8\010\ufff8\001\002" });

  /** Access to parse-action table. */
  public short[][] action_table() {return _action_table;}

  /** <code>reduce_goto</code> table. */
  protected static final short[][] _reduce_table = 
    unpackFromStrings(new String[] {
    "\000\034\000\012\002\005\003\011\004\010\005\007\001" +
    "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
    "\000\002\001\001\000\002\001\001\000\002\001\001\000" +
    "\002\001\001\000\002\001\001\000\002\001\001\000\002" +
    "\001\001\000\002\001\001\000\002\001\001\000\002\001" +
    "\001\000\002\001\001\000\002\001\001\000\006\007\025" +
    "\010\023\001\001\000\002\001\001\000\002\001\001\000" +
    "\004\010\026\001\001\000\002\001\001\000\002\001\001" +
    "\000\002\001\001\000\002\001\001\000\004\006\033\001" +
    "\001\000\002\001\001\000\002\001\001\000\002\001\001" +
    "" });

  /** Access to <code>reduce_goto</code> table. */
  public short[][] reduce_table() {return _reduce_table;}

  /** Instance of action encapsulation class. */
  protected CUP$RecordParser$actions action_obj;

  /** Action encapsulation object initializer. */
  protected void init_actions()
    {
      action_obj = new CUP$RecordParser$actions(this);
    }

  /** Invoke a user supplied parse action. */
  public java_cup.runtime.Symbol do_action(
    int                        act_num,
    java_cup.runtime.lr_parser parser,
    java.util.Stack            stack,
    int                        top)
    throws java.lang.Exception
  {
    /* call code in generated class */
    return action_obj.CUP$RecordParser$do_action(act_num, parser, stack, top);
  }

  /** Indicates start state. */
  public int start_state() {return 0;}
  /** Indicates start production. */
  public int start_production() {return 0;}

  /** <code>EOF</code> Symbol index. */
  public int EOF_sym() {return 0;}

  /** <code>error</code> Symbol index. */
  public int error_sym() {return 1;}


  /** User initialization code. */
  public void user_init() throws java.lang.Exception
    {
               
    }

  /** Scan to get the next Symbol. */
  public java_cup.runtime.Symbol scan()
    throws java.lang.Exception
    {
 return getScanner().next_token(); 
    }
}

/** Cup generated class to encapsulate user supplied action code.*/
class CUP$RecordParser$actions {
  private final RecordParser parser;

  /** Constructor */
  CUP$RecordParser$actions(RecordParser parser) {
    this.parser = parser;
  }

  /** Method with the actual generated action code. */
  public final java_cup.runtime.Symbol CUP$RecordParser$do_action(
    int                        CUP$RecordParser$act_num,
    java_cup.runtime.lr_parser CUP$RecordParser$parser,
    java.util.Stack            CUP$RecordParser$stack,
    int                        CUP$RecordParser$top)
    throws java.lang.Exception
    {
      /* Symbol object for return from actions */
      java_cup.runtime.Symbol CUP$RecordParser$result;

      /* select the action based on the action number */
      switch (CUP$RecordParser$act_num)
        {
          /*. . . . . . . . . . . . . . . . . . . .*/
          case 13: // nodeRef ::= NID 
            {
              Tuple2<String,String> RESULT =null;
		int idleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int idright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		String id = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new Tuple2<String,String>(id, ""); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("nodeRef",6, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 12: // nodeRef ::= NID COLON IP 
            {
              Tuple2<String,String> RESULT =null;
		int idleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).left;
		int idright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).right;
		String id = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).value;
		int ipleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int ipright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		String ip = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new Tuple2<String,String>(id,ip); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("nodeRef",6, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 11: // nodeRefList ::= nodeRefList nodeRef 
            {
              List<Tuple2<String,String>> RESULT =null;
		int nrlleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).left;
		int nrlright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).right;
		List<Tuple2<String,String>> nrl = (List<Tuple2<String,String>>)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).value;
		int nrleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int nrright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		Tuple2<String,String> nr = (Tuple2<String,String>)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = nrl; nrl.add(nr); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("nodeRefList",5, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 10: // nodeRefList ::= nodeRef 
            {
              List<Tuple2<String,String>> RESULT =null;
		int nrleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int nrright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		Tuple2<String,String> nr = (Tuple2<String,String>)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new ArrayList<Tuple2<String,String>>(); RESULT.add(nr); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("nodeRefList",5, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 9: // ipList ::= ipList IP 
            {
              List<String> RESULT =null;
		int illeft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).left;
		int ilright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).right;
		List<String> il = (List<String>)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).value;
		int il1left = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int il1right = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		String il1 = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = il; RESULT.add(il1); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("ipList",4, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 8: // ipList ::= IP 
            {
              List<String> RESULT =null;
		int il1left = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int il1right = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		String il1 = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new ArrayList<String>(); RESULT.add(il1); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("ipList",4, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 7: // nodelink ::= NODELINK NID COLON IP COLON LID 
            {
              NodeLink RESULT =null;
		int nidleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-4)).left;
		int nidright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-4)).right;
		String nid = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-4)).value;
		int ipleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).left;
		int ipright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).right;
		String ip = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).value;
		int lidleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int lidright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		String lid = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new NodeLink(nid, ip, lid); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("nodelink",3, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-5)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 6: // nodelink ::= NODELINK NID COLON LID 
            {
              NodeLink RESULT =null;
		int nidleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).left;
		int nidright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).right;
		String nid = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).value;
		int lidleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int lidright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		String lid = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new NodeLink(nid, "", lid); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("nodelink",3, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-3)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 5: // link ::= LINK LID COLON nodeRefList 
            {
              Link RESULT =null;
		int idleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).left;
		int idright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).right;
		String id = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).value;
		int nrlleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int nrlright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		List<Tuple2<String,String>> nrl = (List<Tuple2<String,String>>)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new Link(id, nrl); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("link",1, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-3)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 4: // node ::= NODE NID COLON ipList 
            {
              Node RESULT =null;
		int idleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).left;
		int idright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).right;
		String id = (String)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-2)).value;
		int illeft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int ilright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		List<String> il = (List<String>)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = new Node(id, il); 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("node",2, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-3)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 3: // record ::= nodelink 
            {
              Object RESULT =null;
		int nlleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int nlright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		NodeLink nl = (NodeLink)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = nl; 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("record",0, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 2: // record ::= link 
            {
              Object RESULT =null;
		int lleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int lright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		Link l = (Link)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = l; 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("record",0, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 1: // record ::= node 
            {
              Object RESULT =null;
		int nleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).left;
		int nright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()).right;
		Node n = (Node)((java_cup.runtime.Symbol) CUP$RecordParser$stack.peek()).value;
		 RESULT = n; 
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("record",0, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          return CUP$RecordParser$result;

          /*. . . . . . . . . . . . . . . . . . . .*/
          case 0: // $START ::= record EOF 
            {
              Object RESULT =null;
		int start_valleft = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).left;
		int start_valright = ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).right;
		Object start_val = (Object)((java_cup.runtime.Symbol) CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)).value;
		RESULT = start_val;
              CUP$RecordParser$result = parser.getSymbolFactory().newSymbol("$START",0, ((java_cup.runtime.Symbol)CUP$RecordParser$stack.elementAt(CUP$RecordParser$top-1)), ((java_cup.runtime.Symbol)CUP$RecordParser$stack.peek()), RESULT);
            }
          /* ACCEPT */
          CUP$RecordParser$parser.done_parsing();
          return CUP$RecordParser$result;

          /* . . . . . .*/
          default:
            throw new Exception(
               "Invalid action number found in internal parse table");

        }
    }
}


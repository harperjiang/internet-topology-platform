/* JFlex example: part of Java language lexer specification */
package edu.clarkson.cs.itop.core.core.parser;

import java_cup.runtime.*;

%%

%class Lexer
%unicode
%line
%column

%cup
%eofval{ 
 return new java_cup.runtime.Symbol(Syms.EOF);
%eofval}

%eofclose
%{
  StringBuffer string = new StringBuffer();

  private Symbol symbol(int type) {
    return new Symbol(type, yyline, yycolumn);
  }
  private Symbol symbol(int type, Object value) {
    return new Symbol(type, yyline, yycolumn, value);
  }
%}

NID = N[0-9]+
LID = L[0-9]+
IP = [0-9]{1,3}"."[0-9]{1,3}"."[0-9]{1,3}"."[0-9]{1,3}
SPACE = [ \t]+
%%
/* rules */
"node"		{ return symbol(Syms.NODE); }
"link"		{ return symbol(Syms.LINK); }
"nodelink"  { return symbol(Syms.NODELINK); }
":"			{ return symbol(Syms.COLON); }
{NID}		{ return symbol(Syms.NID, yytext()); }
{LID}		{ return symbol(Syms.LID, yytext()); }
{IP}		{ return symbol(Syms.IP, yytext()); }
{SPACE}		{  }
.			{ throw new IllegalArgumentException("Unrecognized:" + yytext()); }
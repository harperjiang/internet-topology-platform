package edu.clarkson.cs.itop.core.parser;

import java.io.StringReader;

public class Parser {

	public <T> T  parse(String input) {
		Lexer scanner = new Lexer(new StringReader(input));
		RecordParser parser = new RecordParser(scanner);
		try {
			return (T)parser.parse().value;
		} catch (Exception e) {
			return null;
		}
	}
}
